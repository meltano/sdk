"""Target abstract class."""

import abc
import copy
import json
import sys
import time
from io import FileIO
from pathlib import Path, PurePath
from typing import IO, Callable, Dict, List, Optional, Tuple, Type, Union

import click
from joblib import Parallel, delayed, parallel_backend

from singer_sdk.exceptions import RecordsWitoutSchemaException
from singer_sdk.helpers._classproperty import classproperty
from singer_sdk.helpers._compat import final
from singer_sdk.helpers.capabilities import CapabilitiesEnum, PluginCapabilities
from singer_sdk.mapper import PluginMapper
from singer_sdk.plugin_base import PluginBase
from singer_sdk.sinks import Sink

_MAX_PARALLELISM = 8


class Target(PluginBase, metaclass=abc.ABCMeta):
    """Abstract base class for targets.

    The `Target` class manages config information and is responsible for processing the
    incoming Singer data stream and orchestrating any needed target `Sink` objects. As
    messages are received from the tap, the `Target` class will automatically create
    any needed target `Sink` objects and send records along to the appropriate `Sink`
    object for that record.
    """

    _MAX_RECORD_AGE_IN_MINUTES: float = 30.0

    # Default class to use for creating new sink objects.
    # Required if `Target.get_sink_class()` is not defined.
    default_sink_class: Optional[Type[Sink]] = None

    def __init__(
        self,
        config: Optional[Union[dict, PurePath, str, List[Union[PurePath, str]]]] = None,
        parse_env_config: bool = False,
        validate_config: bool = True,
    ) -> None:
        """Initialize the target.

        Args:
            config: Target configuration. Can be a dictionary, a single path to a
                configuration file, or a list of paths to multiple configuration
                files.
            parse_env_config: Whether to look for configuration values in environment
                variables.
            validate_config: True to require validation of config settings.
        """
        super().__init__(
            config=config,
            parse_env_config=parse_env_config,
            validate_config=validate_config,
        )

        self._latest_state: Dict[str, dict] = {}
        self._drained_state: Dict[str, dict] = {}
        self._sinks_active: Dict[str, Sink] = {}
        self._sinks_to_clear: List[Sink] = []
        self._max_parallelism: Optional[int] = _MAX_PARALLELISM

        # Approximated for max record age enforcement
        self._last_full_drain_at: float = time.time()

        # Initialize mapper
        self.mapper: PluginMapper
        self.mapper = PluginMapper(
            plugin_config=dict(self.config),
            logger=self.logger,
        )

    @classproperty
    def capabilities(self) -> List[CapabilitiesEnum]:
        """Get target capabilites.

        Returns:
            A list of capabilities supported by this target.
        """
        return [
            PluginCapabilities.ABOUT,
            PluginCapabilities.STREAM_MAPS,
        ]

    @property
    def max_parallelism(self) -> int:
        """Get max parallel sinks.

        The default is 8 if not overridden.

        Returns:
            Max number of sinks that can be drained in parallel.
        """
        if self._max_parallelism is not None:
            return self._max_parallelism

        return _MAX_PARALLELISM

    @max_parallelism.setter
    def max_parallelism(self, new_value: int) -> None:
        """Override the default (max) parallelism.

        The default is 8 if not overridden.

        Args:
            new_value: The new max degree of parallelism for this target.
        """
        self._max_parallelism = new_value

    def get_sink(
        self,
        stream_name: str,
        *,
        record: Optional[dict] = None,
        schema: Optional[dict] = None,
        key_properties: Optional[List[str]] = None,
    ) -> Sink:
        """Return a sink for the given stream name.

        A new sink will be created if `schema` is provided and if either `schema` or
        `key_properties` has changed. If so, the old sink becomes archived and held
        until the next drain_all() operation.

        Developers only need to override this method if they want to provide a different
        sink depending on the values within the `record` object. Otherwise, please see
        `default_sink_class` property and/or the `get_sink_class()` method.

        Raises :class:`singer_sdk.exceptions.RecordsWitoutSchemaException` if sink does
        not exist and schema is not sent.

        Args:
            stream_name: Name of the stream.
            record: Record being processed.
            schema: Stream schema.
            key_properties: Primary key of the stream.

        Returns:
            The sink used for this target.
        """
        _ = record  # Custom implementations may use record in sink selection.
        if schema is None:
            self._assert_sink_exists(stream_name)
            return self._sinks_active[stream_name]

        existing_sink = self._sinks_active.get(stream_name, None)
        if not existing_sink:
            return self.add_sink(stream_name, schema, key_properties)

        if (
            existing_sink.schema != schema
            or existing_sink.key_properties != key_properties
        ):
            self.logger.info(
                f"Schema or key properties for '{stream_name}' stream have changed. "
                f"Initializing a new '{stream_name}' sink..."
            )
            self._sinks_to_clear.append(self._sinks_active.pop(stream_name))
            return self.add_sink(stream_name, schema, key_properties)

        return existing_sink

    def get_sink_class(self, stream_name: str) -> Type[Sink]:
        """Get sink for a stream.

        Developers can override this method to return a custom Sink type depending
        on the value of `stream_name`. Optional when `default_sink_class` is set.

        Args:
            stream_name: Name of the stream.

        Raises:
            ValueError: If no :class:`singer_sdk.sinks.Sink` class is defined.

        Returns:
            The sink class to be used with the stream.
        """
        if self.default_sink_class:
            return self.default_sink_class

        raise ValueError(
            f"No sink class defined for '{stream_name}' "
            "and no default sink class available."
        )

    def sink_exists(self, stream_name: str) -> bool:
        """Check sink for a stream.

        This method is internal to the SDK and should not need to be overridden.

        Args:
            stream_name: Name of the stream

        Returns:
            True if a sink has been initialized.
        """
        return stream_name in self._sinks_active

    @final
    def listen(self, input: Optional[IO[str]] = None) -> None:
        """Read from input until all messages are processed.

        Args:
            input: Readable stream of messages. Defaults to standard in.

        This method is internal to the SDK and should not need to be overridden.
        """
        if not input:
            input = sys.stdin

        self._process_lines(input)
        self._process_endofpipe()

    @final
    def add_sink(
        self, stream_name: str, schema: dict, key_properties: Optional[List[str]] = None
    ) -> Sink:
        """Create a sink and register it.

        This method is internal to the SDK and should not need to be overridden.

        Args:
            stream_name: Name of the stream.
            schema: Schema of the stream.
            key_properties: Primary key of the stream.

        Returns:
            A new sink for the stream.
        """
        self.logger.info(f"Initializing '{self.name}' target sink...")
        sink_class = self.get_sink_class(stream_name=stream_name)
        result = sink_class(
            target=self,
            stream_name=stream_name,
            schema=schema,
            key_properties=key_properties,
        )
        self._sinks_active[stream_name] = result
        return result

    @staticmethod
    def _assert_line_requires(line_dict: dict, requires: List[str]) -> None:
        """TODO.

        Args:
            line_dict: TODO
            requires: TODO

        Raises:
            Exception: TODO
        """
        for required in requires:
            if required not in line_dict:
                raise Exception(
                    f"Line is missing required '{required}' key: {line_dict}"
                )

    def _assert_sink_exists(self, stream_name: str) -> None:
        """Raise a RecordsWitoutSchemaException exception if stream doesn't exist.

        Args:
            stream_name: TODO

        Raises:
            RecordsWitoutSchemaException: If sink does not exist and schema is not sent.
        """
        if not self.sink_exists(stream_name):
            raise RecordsWitoutSchemaException(
                f"A record for stream '{stream_name}' was encountered before a "
                "corresponding schema."
            )

    # Message handling

    def _process_lines(self, input: IO[str]) -> None:
        """Internal method to process jsonl lines from a Singer tap.

        Args:
            input: Readable stream of messages, each on a separate line.

        Raises:
            json.decoder.JSONDecodeError: raised if any lines are not valid json
            ValueError: raised if a message type is not recognized
        """
        self.logger.info(f"Target '{self.name}' is listening for input from tap.")
        line_counter = 0
        record_counter = 0
        state_counter = 0
        for line in input:
            line_counter += 1
            try:
                line_dict = json.loads(line)
            except json.decoder.JSONDecodeError:
                self.logger.error("Unable to parse:\n{}".format(line))
                raise

            self._assert_line_requires(line_dict, requires=["type"])

            record_type = line_dict["type"]
            if record_type == "SCHEMA":
                self._process_schema_message(line_dict)
                continue

            if record_type == "RECORD":
                self._process_record_message(line_dict)
                record_counter += 1
                continue

            if record_type == "ACTIVATE_VERSION":
                self._process_activate_version_message(line_dict)
                continue

            if record_type == "STATE":
                self._process_state_message(line_dict)
                state_counter += 1
                continue

            raise ValueError(f"Unknown message type '{record_type}' in message.")

        self.logger.info(
            f"Target '{self.name}' completed reading {line_counter} lines of input "
            f"({record_counter} records, {state_counter} state messages)."
        )

    def _process_endofpipe(self) -> None:
        """Called after all input lines have been read."""
        self.drain_all()

    def _process_record_message(self, message_dict: dict) -> None:
        """Process a RECORD message.

        Args:
            message_dict: TODO
        """
        self._assert_line_requires(message_dict, requires=["stream", "record"])

        stream_name = message_dict["stream"]
        for stream_map in self.mapper.stream_maps[stream_name]:
            # new_schema = helpers._float_to_decimal(new_schema)
            raw_record = copy.copy(message_dict["record"])
            transformed_record = stream_map.transform(raw_record)
            if transformed_record is None:
                # Record was filtered out by the map transform
                continue

            sink = self.get_sink(stream_map.stream_alias, record=transformed_record)
            context = sink._get_context(transformed_record)
            if sink.include_sdc_metadata_properties:
                sink._add_sdc_metadata_to_record(
                    transformed_record, message_dict, context
                )
            else:
                sink._remove_sdc_metadata_from_record(transformed_record)

            sink._validate_and_parse(transformed_record)

            sink.tally_record_read()
            transformed_record = sink.preprocess_record(transformed_record, context)
            sink.process_record(transformed_record, context)
            sink._after_process_record(context)

            if sink.is_full:
                self.logger.info(
                    f"Target sink for '{sink.stream_name}' is full. Draining..."
                )
                self.drain_one(sink)

    def _process_schema_message(self, message_dict: dict) -> None:
        """Process a SCHEMA messages.

        Args:
            message_dict: TODO
        """
        self._assert_line_requires(message_dict, requires=["stream", "schema"])

        stream_name = message_dict["stream"]
        schema = message_dict["schema"]
        key_properties = message_dict.get("key_properties", None)
        if stream_name not in self.mapper.stream_maps:
            self.mapper.register_raw_stream_schema(
                stream_name,
                schema,
                key_properties,
            )
        for stream_map in self.mapper.stream_maps[stream_name]:
            # new_schema = helpers._float_to_decimal(new_schema)
            _ = self.get_sink(
                stream_map.stream_alias,
                schema=stream_map.transformed_schema,
                key_properties=stream_map.transformed_key_properties,
            )

    @property
    def _max_record_age_in_minutes(self) -> float:
        return (time.time() - self._last_full_drain_at) / 60

    def _reset_max_record_age(self) -> None:
        self._last_full_drain_at = time.time()

    def _process_state_message(self, message_dict: dict) -> None:
        """Process a state message. drain sinks if needed.

        If state is unchanged, no actions will be taken.

        Args:
            message_dict: TODO
        """
        self._assert_line_requires(message_dict, requires=["value"])
        state = message_dict["value"]
        if self._latest_state == state:
            return
        self._latest_state = state
        if self._max_record_age_in_minutes > self._MAX_RECORD_AGE_IN_MINUTES:
            self.logger.info(
                "One or more records have exceeded the max age of "
                f"{self._MAX_RECORD_AGE_IN_MINUTES} minutes. Draining all sinks."
            )
            self.drain_all()

    def _process_activate_version_message(self, message_dict: dict) -> None:
        """Handle the optional ACTIVATE_VERSION message extension.

        Args:
            message_dict: TODO
        """
        self.logger.warning(
            "ACTIVATE_VERSION message received but not supported. Ignoring."
            "For more information: https://gitlab.com/meltano/sdk/-/issues/18"
        )

    # Sink drain methods

    @final
    def drain_all(self) -> None:
        """Drains all sinks, starting with those cleared due to changed schema.

        This method is internal to the SDK and should not need to be overridden.
        """
        state = copy.deepcopy(self._latest_state)
        self._drain_all(self._sinks_to_clear, 1)
        self._sinks_to_clear = []
        self._drain_all(list(self._sinks_active.values()), self.max_parallelism)
        self._write_state_message(state)
        self._reset_max_record_age()

    @final
    def drain_one(self, sink: Sink) -> None:
        """Drain a specific sink.

        This method is internal to the SDK and should not need to be overridden.

        Args:
            sink: Sink to be drained.
        """
        if sink.current_size == 0:
            return

        draining_status = sink.start_drain()
        sink.process_batch(draining_status)
        sink.mark_drained()

    def _drain_all(self, sink_list: List[Sink], parallelism: int) -> None:
        if parallelism == 1:
            for sink in sink_list:
                self.drain_one(sink)
            return

        def _drain_sink(sink: Sink) -> None:
            self.drain_one(sink)

        with parallel_backend("threading", n_jobs=parallelism):
            Parallel()(delayed(_drain_sink)(sink=sink) for sink in sink_list)

    def _write_state_message(self, state: dict) -> None:
        """Emit the stream's latest state.

        Args:
            state: TODO
        """
        state_json = json.dumps(state)
        self.logger.info(f"Emitting completed target state {state_json}")
        sys.stdout.write("{}\n".format(state_json))
        sys.stdout.flush()

    # CLI handler

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
            "--input",
            help="A path to read messages from instead of from standard in.",
            type=click.File("r"),
        )
        @click.command(
            help="Execute the Singer target.",
            context_settings={"help_option_names": ["--help"]},
        )
        def cli(
            version: bool = False,
            about: bool = False,
            config: Tuple[str, ...] = (),
            format: str = None,
            input: FileIO = None,
        ) -> None:
            """Handle command line execution.

            Args:
                version: Display the package version.
                about: Display package metadata and settings.
                format: Specify output style for `--about`.
                config: Configuration file location or 'ENV' to use environment
                    variables. Accepts multiple inputs as a tuple.
                input: Specify a path to an input file to read messages from.
                    Defaults to standard in if unspecified.

            Raises:
                FileNotFoundError: If the config file does not exist.
            """
            if version:
                cls.print_version()
                return

            if about:
                cls.print_about(format)
                return

            cls.print_version(print_fn=cls.logger.info)

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

            target = cls(  # type: ignore  # Ignore 'type not callable'
                config=config_files or None,
                parse_env_config=parse_env_config,
            )
            target.listen(input)

        return cli
