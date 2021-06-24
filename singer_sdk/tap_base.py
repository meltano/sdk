"""Tap abstract class."""

import abc
import json
from pathlib import PurePath, Path
from singer_sdk.mapper import PluginMapper
from typing import Any, List, Optional, Dict, Type, Union, cast

import click
from singer.catalog import Catalog

from singer_sdk.helpers._classproperty import classproperty
from singer_sdk.helpers._compat import final
from singer_sdk.helpers._util import read_json_file
from singer_sdk.helpers._state import write_stream_state
from singer_sdk.plugin_base import PluginBase
from singer_sdk.streams.core import Stream
from singer_sdk.exceptions import (
    MaxRecordsLimitException,
)
from singer_sdk.helpers import _state

STREAM_MAPS_CONFIG = "stream_maps"


class Tap(PluginBase, metaclass=abc.ABCMeta):
    """Abstract base class for taps.

    The Tap class governs configuration, validation, and stream discovery for tap
    plugins.
    """

    # Constructor

    def __init__(
        self,
        config: Optional[Union[dict, PurePath, str, List[Union[PurePath, str]]]] = None,
        catalog: Union[PurePath, str, dict, None] = None,
        state: Union[PurePath, str, dict, None] = None,
        parse_env_config: bool = False,
    ) -> None:
        """Initialize the tap."""
        super().__init__(config=config, parse_env_config=parse_env_config)

        # Declare private members
        self._streams: Optional[Dict[str, Stream]] = None
        self._input_catalog: Optional[dict] = None
        self._state: Dict[str, Stream] = {}

        # Process input catalog
        if isinstance(catalog, dict):
            self._input_catalog = catalog
        elif catalog is not None:
            self._input_catalog = read_json_file(catalog)

        # Initialize mapper
        self.mapper: PluginMapper
        self.mapper = PluginMapper(
            plugin_config=dict(self.config),
            logger=self.logger,
        )
        self.mapper.register_raw_streams_from_catalog(
            self._input_catalog or self.catalog_dict
        )

        # Process state
        state_dict: dict = {}
        if isinstance(state, dict):
            state_dict = state
        elif state:
            state_dict = read_json_file(state)
        self.load_state(state_dict)

    # Class properties

    @property
    def streams(self) -> Dict[str, Stream]:
        """Return a list of streams, using discovery or a provided catalog.

        Results will be cached after first execution.
        """
        if self._streams is None:
            self._streams = {}
            for stream in self.load_streams():
                if self.input_catalog:
                    stream.apply_catalog(self.input_catalog)
                self._streams[stream.name] = stream
        return self._streams

    @property
    def state(self) -> dict:
        """Return a state dict."""
        if self._state is None:
            raise RuntimeError("Could not read from uninitialized state.")
        return self._state

    @property
    def input_catalog(self) -> Optional[dict]:
        """Return the catalog dictionary input, or None if not provided."""
        return self._input_catalog

    @classproperty
    def capabilities(self) -> List[str]:
        """Return a list of supported capabilities."""
        return ["sync", "catalog", "state", "discover"]

    # Connection test:

    @final
    def run_connection_test(self) -> bool:
        """Run connection test and return True if successful."""
        for stream in self.streams.values():
            if stream.parent_stream_type:
                self.logger.debug(
                    f"Child stream '{type(stream).__name__}' should be called by "
                    f"parent stream '{stream.parent_stream_type.__name__}'. "
                    "Skipping direct invocation."
                )
                continue

            stream._MAX_RECORDS_LIMIT = 1 if stream.child_streams else 0
            try:
                stream.sync()
            except MaxRecordsLimitException:
                pass
        return True

    # Stream detection:

    def run_discovery(self) -> str:
        """Write the catalog json to STDOUT and return the same as a string."""
        catalog_text = self.catalog_json_text
        print(catalog_text)
        return catalog_text

    @property
    def catalog_dict(self) -> dict:
        """Return the tap's catalog as a dict."""
        return cast(dict, self._singer_catalog.to_dict())

    @property
    def catalog_json_text(self) -> str:
        """Return the tap's catalog as formatted json text."""
        return json.dumps(self.catalog_dict, indent=2)

    @property
    def _singer_catalog(self) -> Catalog:
        """Return a Catalog object."""
        catalog_entries = [
            stream._singer_catalog_entry for stream in self.streams.values()
        ]
        return Catalog(catalog_entries)

    def discover_streams(self) -> List[Stream]:
        """Initialize all available streams and return them as a list."""
        raise NotImplementedError(
            f"Tap '{self.name}' does not support discovery. "
            "Please set the '--catalog' command line argument and try again."
        )

    @final
    def load_streams(self) -> List[Stream]:
        """Load streams from discovery and initialize DAG.

        Return the output of `self.discover_streams()` to enumerate
        discovered streams.
        """
        # Build the parent-child dependency DAG

        # Index streams by type
        streams_by_type: Dict[Type[Stream], List[Stream]] = {}
        for stream in self.discover_streams():
            stream_type = type(stream)
            if stream_type not in streams_by_type:
                streams_by_type[stream_type] = []
            streams_by_type[stream_type].append(stream)

        # Initialize child streams list for parents
        for stream_type, streams in streams_by_type.items():
            if stream_type.parent_stream_type:
                parents = streams_by_type[stream_type.parent_stream_type]
                for parent in parents:
                    for stream in streams:
                        parent.child_streams.append(stream)
                        self.logger.info(
                            f"Added '{stream.name}' as child stream to '{parent.name}'"
                        )

        streams = [stream for streams in streams_by_type.values() for stream in streams]
        return sorted(
            streams,
            key=lambda x: x.name,
            reverse=False,
        )

    # Bookmarks and state management

    def load_state(self, state: Dict[str, Any]) -> None:
        """Merge or initialize stream state with the provided state dictionary input.

        Override this method to perform validation and backwards-compatibility patches
        on self.state. If overriding, we recommend first running
        `super().load_state(state)` to ensure compatibility with the SDK.
        """
        if self.state is None:
            raise ValueError("Cannot write to uninitialized state dictionary.")

        for stream_name, stream_state in state.get("bookmarks", {}).items():
            for key, val in stream_state.items():
                write_stream_state(
                    self.state,
                    stream_name,
                    key,
                    val,
                )

    # State handling

    def _reset_state_progress_markers(self) -> None:
        """Clear prior jobs' progress markers at beginning of sync."""
        for stream_name, state in self.state.get("bookmarks", {}).items():
            _state.reset_state_progress_markers(state)
            for partition_state in state.get("partitions", []):
                _state.reset_state_progress_markers(partition_state)

    # Fix sync replication method incompatibilities

    def _set_compatible_replication_methods(self) -> None:
        stream: Stream
        for stream in self.streams.values():
            for descendent in stream.descendent_streams:
                if descendent.selected and descendent.ignore_parent_replication_key:
                    self.logger.warning(
                        f"Stream descendent '{descendent.name}' is selected and "
                        f"its parent '{stream.name}' does not use inclusive "
                        f"replication keys. "
                        f"Forcing full table replication for '{stream.name}'."
                    )
                    stream.replication_key = None
                    stream.forced_replication_method = "FULL_TABLE"

    # Sync methods

    @final
    def sync_all(self):
        """Sync all streams."""
        self._reset_state_progress_markers()
        self._set_compatible_replication_methods()
        stream: "Stream"
        for stream in self.streams.values():
            if not stream.selected and not stream.has_selected_descendents:
                self.logger.info(f"Skipping deselected stream '{stream.name}'.")
                continue

            if stream.parent_stream_type:
                self.logger.debug(
                    f"Child stream '{type(stream).__name__}' is expected to be called "
                    f"by parent stream '{stream.parent_stream_type.__name__}'. "
                    "Skipping direct invocation."
                )
                continue

            stream.sync()
            stream.finalize_state_progress_markers()

    # Command Line Execution

    @classproperty
    def cli(cls):
        """Execute standard CLI handler for taps."""

        @click.option("--version", is_flag=True)
        @click.option("--about", is_flag=True)
        @click.option("--discover", is_flag=True)
        @click.option("--test", is_flag=True)
        @click.option("--format")
        @click.option("--config", multiple=True)
        @click.option("--catalog")
        @click.option("--state")
        @click.command()
        def cli(
            version: bool = False,
            about: bool = False,
            discover: bool = False,
            test: bool = False,
            config: List[str] = None,
            state: str = None,
            catalog: str = None,
            format: str = None,
        ) -> None:
            """Handle command line execution."""
            if version:
                cls.print_version()
                return

            if about:
                cls.print_about(format)
                return

            cls.print_version(print_fn=cls.logger.info)

            parse_env_config = False
            config_files: List[PurePath] = []
            for config_path in config or []:
                if config_path == "ENV":
                    # Allow parse from env vars:
                    parse_env_config = True
                    continue

                # Validate config file paths before adding to list
                if not Path(config_path).is_file():
                    raise FileNotFoundError(
                        f"Could not locate config file at '{config_path}'."
                        "Please check that the file exists."
                    )

                config_files.append(Path(config_path))

            tap = cls(  # type: ignore  # Ignore 'type not callable'
                config=config_files or None,
                state=state,
                catalog=catalog,
                parse_env_config=parse_env_config,
            )
            if discover:
                tap.run_discovery()
                if test:
                    tap.run_connection_test()
            elif test:
                tap.run_connection_test()
            else:
                tap.sync_all()

        return cli
