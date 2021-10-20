"""Tap abstract class."""

import abc
import json
from pathlib import Path, PurePath
from typing import Any, Callable, Dict, List, Optional, Tuple, Type, Union, cast

import click

from singer_sdk.exceptions import MaxRecordsLimitException
from singer_sdk.helpers import _state
from singer_sdk.helpers._classproperty import classproperty
from singer_sdk.helpers._compat import final
from singer_sdk.helpers._singer import Catalog
from singer_sdk.helpers._state import write_stream_state
from singer_sdk.helpers._util import read_json_file
from singer_sdk.mapper import PluginMapper
from singer_sdk.plugin_base import PluginBase
from singer_sdk.streams.core import Stream

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
        validate_config: bool = True,
    ) -> None:
        """Initialize the tap.

        Args:
            config: Tap configuration. Can be a dictionary, a single path to a
                configuration file, or a list of paths to multiple configuration
                files.
            catalog: Tap catalog. Can be a dictionary or a path to the catalog file.
            state: Tap state. Can be dictionary or a path to the state file.
            parse_env_config: Whether to look for configuration values in environment
                variables.
            validate_config: True to require validation of config settings.
        """
        super().__init__(
            config=config,
            parse_env_config=parse_env_config,
            validate_config=validate_config,
        )

        # Declare private members
        self._streams: Optional[Dict[str, Stream]] = None
        self._input_catalog: Optional[Catalog] = None
        self._state: Dict[str, Stream] = {}

        # Process input catalog
        if isinstance(catalog, dict):
            self._input_catalog = Catalog.from_dict(catalog)
        elif catalog is not None:
            self._input_catalog = Catalog.from_dict(read_json_file(catalog))

        # Initialize mapper
        self.mapper: PluginMapper
        self.mapper = PluginMapper(
            plugin_config=dict(self.config),
            logger=self.logger,
        )
        self.mapper.register_raw_streams_from_catalog(
            self.input_catalog or self._singer_catalog
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
        """Get streams discovered or catalogued for this tap.

        Results will be cached after first execution.

        Returns:
            A mapping of names to streams, using discovery or a provided catalog.
        """
        input_catalog = self.input_catalog

        if self._streams is None:
            self._streams = {}
            for stream in self.load_streams():
                if input_catalog is not None:
                    stream.apply_catalog(input_catalog)
                self._streams[stream.name] = stream
        return self._streams

    @property
    def state(self) -> dict:
        """Get tap state.

        Returns:
            The tap's state dictionary

        Raises:
            RuntimeError: If state has not been initialized.
        """
        if self._state is None:
            raise RuntimeError("Could not read from uninitialized state.")
        return self._state

    @property
    def input_catalog(self) -> Optional[Catalog]:
        """Get the catalog passed to the tap.

        Returns:
            Catalog dictionary input, or None if not provided.
        """
        return self._input_catalog

    @classproperty
    def capabilities(self) -> List[str]:
        """Get tap capabilities.

        Returns:
            A list of capabilities supported by this tap.
        """
        return ["sync", "catalog", "state", "discover"]

    # Connection test:

    @final
    def run_connection_test(self) -> bool:
        """Run connection test.

        Returns:
            True if the test succeeded.
        """
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
        """Write the catalog json to STDOUT and return as a string.

        Returns:
            The catalog as a string of JSON.
        """
        catalog_text = self.catalog_json_text
        print(catalog_text)
        return catalog_text

    @property
    def catalog_dict(self) -> dict:
        """Get catalog dictionary.

        Returns:
            The tap's catalog as a dict
        """
        return cast(dict, self._singer_catalog.to_dict())

    @property
    def catalog_json_text(self) -> str:
        """Get catalog JSON.

        Returns:
            The tap's catalog as formatted JSON text.
        """
        return json.dumps(self.catalog_dict, indent=2)

    @property
    def _singer_catalog(self) -> Catalog:
        """Return a Catalog object.

        Returns:
            :class:`singer_sdk.helpers._singer.Catalog`.
        """
        return Catalog(
            (stream.tap_stream_id, stream._singer_catalog_entry)
            for stream in self.streams.values()
        )

    def discover_streams(self) -> List[Stream]:
        """Initialize all available streams and return them as a list.

        Raises:
            NotImplementedError: If the tap implementation does not override this
                method.
        """
        raise NotImplementedError(
            f"Tap '{self.name}' does not support discovery. "
            "Please set the '--catalog' command line argument and try again."
        )

    @final
    def load_streams(self) -> List[Stream]:
        """Load streams from discovery and initialize DAG.

        Return the output of `self.discover_streams()` to enumerate
        discovered streams.

        Returns:
            A list of discovered streams, ordered by name.
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

        Args:
            state: Initialize the tap'ss state with this value.

        Raises:
            ValueError: If the tap's own state is None, meaning it has not been
                initialized.
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
    def sync_all(self) -> None:
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
    def cli(cls) -> Callable:
        """Execute standard CLI handler for taps.

        Returns:
            A callable CLI object.
        """

        @click.option(
            "--version",
            is_flag=True,
            help="Display the package version.",
        )
        @click.option(
            "--about",
            is_flag=True,
            help="Display package metadata and settings.",
        )
        @click.option(
            "--discover",
            is_flag=True,
            help="Run the tap in discovery mode.",
        )
        @click.option(
            "--test",
            is_flag=True,
            help="Test connectivity by syncing a single record and exiting.",
        )
        @click.option(
            "--format",
            help="Specify output style for --about",
            type=click.Choice(["json", "markdown"], case_sensitive=False),
            default=None,
        )
        @click.option(
            "--config",
            multiple=True,
            help="Configuration file location or 'ENV' to use environment variables.",
            type=click.STRING,
            default=(),
        )
        @click.option(
            "--catalog",
            help="Use a Singer catalog file with the tap.",
            type=click.Path(),
        )
        @click.option(
            "--state",
            help="Use a bookmarks file for incremental replication.",
            type=click.Path(),
        )
        @click.command(
            help="Execute the Singer tap.",
            context_settings={"help_option_names": ["--help"]},
        )
        def cli(
            version: bool = False,
            about: bool = False,
            discover: bool = False,
            test: bool = False,
            config: Tuple[str, ...] = (),
            state: str = None,
            catalog: str = None,
            format: str = None,
        ) -> None:
            """Handle command line execution.

            Args:
                version: Display the package version.
                about: Display package metadata and settings.
                discover: Run the tap in discovery mode.
                test: Test connectivity by syncing a single record and exiting.
                format: Specify output style for `--about`.
                config: Configuration file location or 'ENV' to use environment
                    variables.
                catalog: Use a Singer catalog file with the tap.",
                state: Use a bookmarks file for incremental replication.

            Raises:
                FileNotFoundError: If the config file does not exist.
            """
            if version:
                cls.print_version()
                return

            if not about:
                cls.print_version(print_fn=cls.logger.info)

            validate_config: bool = True
            if about or discover:
                # Don't abort on validation failures
                validate_config = False

            parse_env_config = False
            config_files: List[PurePath] = []
            for config_path in config:
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
                validate_config=validate_config,
            )
            if about:
                tap.print_about()
            elif discover:
                tap.run_discovery()
                if test:
                    tap.run_connection_test()
            elif test:
                tap.run_connection_test()
            else:
                tap.sync_all()

        return cli
