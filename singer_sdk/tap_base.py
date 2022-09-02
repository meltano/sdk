"""Tap abstract class."""

import abc
import json
from enum import Enum
from pathlib import PurePath
from typing import Any, Dict, List, Optional, Sequence, Tuple, Type, Union, cast

import click

from singer_sdk._python_types import _FilePath
from singer_sdk.exceptions import MaxRecordsLimitException
from singer_sdk.helpers import _state
from singer_sdk.helpers._classproperty import classproperty
from singer_sdk.helpers._compat import final
from singer_sdk.helpers._singer import Catalog
from singer_sdk.helpers._state import write_stream_state
from singer_sdk.helpers._util import read_json_file
from singer_sdk.helpers.capabilities import (
    CapabilitiesEnum,
    PluginCapabilities,
    TapCapabilities,
)
from singer_sdk.mapper import PluginMapper
from singer_sdk.plugin_base import PluginBase
from singer_sdk.streams import SQLStream, Stream

STREAM_MAPS_CONFIG = "stream_maps"


class CliTestOptionValue(Enum):
    """Values for CLI option --test."""

    All = "all"
    Schema = "schema"
    Disabled = "disabled"


class Tap(PluginBase, metaclass=abc.ABCMeta):
    """Abstract base class for taps.

    The Tap class governs configuration, validation, and stream discovery for tap
    plugins.
    """

    dynamic_catalog: bool = False
    """Whether the tap's catalog is dynamic. Set to True if the catalog is
    generated dynamically (e.g. by querying a database's system tables)."""

    # Constructor

    def __init__(
        self,
        config: Optional[Union[dict, _FilePath, Sequence[_FilePath]]] = None,
        catalog: Union[PurePath, str, dict, Catalog, None] = None,
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
        self._catalog: Optional[Catalog] = None  # Tap's working catalog

        # Process input catalog
        if isinstance(catalog, Catalog):
            self._input_catalog = catalog
        elif isinstance(catalog, dict):
            self._input_catalog = Catalog.from_dict(catalog)
        elif catalog is not None:
            self._input_catalog = Catalog.from_dict(read_json_file(catalog))

        # Initialize mapper
        self.mapper: PluginMapper
        self.mapper = PluginMapper(
            plugin_config=dict(self.config),
            logger=self.logger,
        )

        self.mapper.register_raw_streams_from_catalog(self.catalog)

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

    @property
    def catalog(self) -> Catalog:
        """Get the tap's working catalog.

        Returns:
            A Singer catalog object.
        """
        if self._catalog is None:
            self._catalog = self.input_catalog or self._singer_catalog

        return self._catalog

    @classproperty
    def capabilities(self) -> List[CapabilitiesEnum]:
        """Get tap capabilities.

        Returns:
            A list of capabilities supported by this tap.
        """
        return [
            TapCapabilities.CATALOG,
            TapCapabilities.STATE,
            TapCapabilities.DISCOVER,
            PluginCapabilities.ABOUT,
            PluginCapabilities.STREAM_MAPS,
            PluginCapabilities.FLATTENING,
        ]

    # Connection test:

    @final
    def run_connection_test(self) -> bool:
        """Run connection test.

        Returns:
            True if the test succeeded.
        """
        for stream in self.streams.values():
            # Initialize streams' record limits before beginning the sync test.
            stream._MAX_RECORDS_LIMIT = 1

        for stream in self.streams.values():
            if stream.parent_stream_type:
                self.logger.debug(
                    f"Child stream '{type(stream).__name__}' should be called by "
                    f"parent stream '{stream.parent_stream_type.__name__}'. "
                    "Skipping direct invocation."
                )
                continue
            try:
                stream.sync()
            except MaxRecordsLimitException:
                pass
        return True

    @final
    def write_schemas(self) -> None:
        """Write a SCHEMA message for all known streams to STDOUT."""
        for stream in self.streams.values():
            stream._write_schema_message()

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

        Return:
            List of discovered Stream objects.

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
            state: Initialize the tap's state with this value.

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
        for _, state in self.state.get("bookmarks", {}).items():
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

        # this second loop is needed for all streams to print out their costs
        # including child streams which are otherwise skipped in the loop above
        for stream in self.streams.values():
            stream.log_sync_costs()

    # Command Line Execution

    @classmethod
    def invoke(  # type: ignore[override]
        cls: Type["Tap"],
        config: Tuple[str, ...] = (),
        state: str = None,
        catalog: str = None,
    ) -> None:
        """Invoke the tap's command line interface.

        Args:
            config: Configuration file location or 'ENV' to use environment
                variables. Accepts multiple inputs as a tuple.
            catalog: Use a Singer catalog file with the tap.",
            state: Use a bookmarks file for incremental replication.
        """
        cls.print_version(print_fn=cls.logger.info)
        config_files, parse_env_config = cls.config_from_cli_args(*config)

        tap = cls(
            config=config_files,
            state=state,
            catalog=catalog,
            parse_env_config=parse_env_config,
            validate_config=True,
        )
        tap.sync_all()

    @classmethod
    def cb_discover(
        cls: Type["Tap"],
        ctx: click.Context,
        param: click.Option,
        value: bool,
    ) -> None:
        """CLI callback to run the tap in discovery mode.

        Args:
            ctx: Click context.
            param: Click option.
            value: Whether to run in discovery mode.
        """
        if not value:
            return

        config_args = ctx.params.get("config", ())
        config_files, parse_env_config = cls.config_from_cli_args(*config_args)
        tap = cls(
            config=config_files,
            parse_env_config=parse_env_config,
            validate_config=cls.dynamic_catalog,
        )
        tap.run_discovery()
        ctx.exit()

    @classmethod
    def cb_test(
        cls: Type["Tap"],
        ctx: click.Context,
        param: click.Option,
        value: bool,
    ) -> None:
        """CLI callback to run the tap in test mode.

        Args:
            ctx: Click context.
            param: Click option.
            value: Whether to run in test mode.
        """
        if value == CliTestOptionValue.Disabled.value:
            return

        config_args = ctx.params.get("config", ())
        config_files, parse_env_config = cls.config_from_cli_args(*config_args)
        tap = cls(
            config=config_files,
            parse_env_config=parse_env_config,
            validate_config=True,
        )

        if value == CliTestOptionValue.Schema.value:
            tap.write_schemas()
        else:
            tap.run_connection_test()

        ctx.exit()

    @classmethod
    def get_command(cls: Type["Tap"]) -> click.Command:
        """Execute standard CLI handler for taps.

        Returns:
            A click.Command object.
        """
        command = super().get_command()
        command.help = "Execute the Singer tap."
        command.params.extend(
            [
                click.Option(
                    ["--discover"],
                    is_flag=True,
                    help="Run the tap in discovery mode.",
                    callback=cls.cb_discover,
                    expose_value=False,
                ),
                click.Option(
                    ["--test"],
                    is_flag=False,
                    flag_value=CliTestOptionValue.All.value,
                    default=CliTestOptionValue.Disabled.value,
                    help=(
                        "Use --test to sync a single record for each stream. "
                        + "Use --test=schema to test schema output without syncing "
                        + "records."
                    ),
                    callback=cls.cb_test,
                    expose_value=False,
                ),
                click.Option(
                    ["--catalog"],
                    help="Use a Singer catalog file with the tap.",
                    type=click.Path(),
                ),
                click.Option(
                    ["--state"],
                    help="Use a bookmarks file for incremental replication.",
                    type=click.Path(),
                ),
            ],
        )

        return command


class SQLTap(Tap):
    """A specialized Tap for extracting from SQL streams."""

    # Stream class used to initialize new SQL streams from their catalog declarations.
    default_stream_class: Type[SQLStream]
    dynamic_catalog: bool = True

    def __init__(
        self,
        config: Optional[Union[dict, _FilePath, Sequence[_FilePath]]] = None,
        catalog: Union[PurePath, str, dict, None] = None,
        state: Union[PurePath, str, dict, None] = None,
        parse_env_config: bool = False,
        validate_config: bool = True,
    ) -> None:
        """Initialize the SQL tap.

        The SQLTap initializer additionally creates a cache variable for _catalog_dict.

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
        self._catalog_dict: Optional[dict] = None
        super().__init__(
            config=config,
            catalog=catalog,
            state=state,
            parse_env_config=parse_env_config,
            validate_config=validate_config,
        )

    @property
    def catalog_dict(self) -> dict:
        """Get catalog dictionary.

        Returns:
            The tap's catalog as a dict
        """
        if self._catalog_dict:
            return self._catalog_dict

        if self.input_catalog:
            return self.input_catalog.to_dict()

        connector = self.default_stream_class.connector_class(dict(self.config))

        result: Dict[str, List[dict]] = {"streams": []}
        result["streams"].extend(connector.discover_catalog_entries())

        self._catalog_dict = result
        return self._catalog_dict

    def discover_streams(self) -> List[Stream]:
        """Initialize all available streams and return them as a list.

        Returns:
            List of discovered Stream objects.
        """
        result: List[Stream] = []
        for catalog_entry in self.catalog_dict["streams"]:
            result.append(self.default_stream_class(self, catalog_entry))

        return result
