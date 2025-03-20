"""Target abstract class."""

from __future__ import annotations

import abc
import copy
import json
import sys
import time
import typing as t

import click
from joblib import Parallel, delayed, parallel_config

from singer_sdk.exceptions import RecordsWithoutSchemaException
from singer_sdk.helpers._batch import BaseBatchFileEncoding
from singer_sdk.helpers._classproperty import classproperty
from singer_sdk.helpers.capabilities import (
    ACTIVATE_VERSION_CONFIG,
    ADD_RECORD_METADATA_CONFIG,
    BATCH_CONFIG,
    TARGET_BATCH_SIZE_ROWS_CONFIG,
    TARGET_HARD_DELETE_CONFIG,
    TARGET_LOAD_METHOD_CONFIG,
    TARGET_SCHEMA_CONFIG,
    TARGET_VALIDATE_RECORDS_CONFIG,
    CapabilitiesEnum,
    PluginCapabilities,
    TargetCapabilities,
)
from singer_sdk.io_base import SingerReader
from singer_sdk.plugin_base import BaseSingerReader

if t.TYPE_CHECKING:
    from pathlib import PurePath

    from singer_sdk.connectors import SQLConnector
    from singer_sdk.mapper import PluginMapper
    from singer_sdk.singerlib.encoding.base import GenericSingerReader
    from singer_sdk.sinks import Sink, SQLSink

_MAX_PARALLELISM = 8


class Target(BaseSingerReader, metaclass=abc.ABCMeta):
    """Abstract base class for targets.

    The `Target` class manages config information and is responsible for processing the
    incoming Singer data stream and orchestrating any needed target `Sink` objects. As
    messages are received from the tap, the `Target` class will automatically create
    any needed target `Sink` objects and send records along to the appropriate `Sink`
    object for that record.
    """

    _MAX_RECORD_AGE_IN_MINUTES: float = 5.0

    # Default class to use for creating new sink objects.
    # Required if `Target.get_sink_class()` is not defined.
    default_sink_class: type[Sink]

    message_reader_class: type[GenericSingerReader] = SingerReader
    """The message reader class to use for reading messages."""

    def __init__(
        self,
        *,
        config: dict | PurePath | str | list[PurePath | str] | None = None,
        parse_env_config: bool = False,
        validate_config: bool = True,
        setup_mapper: bool = True,
        message_reader: GenericSingerReader | None = None,
    ) -> None:
        """Initialize the target.

        Args:
            config: Target configuration. Can be a dictionary, a single path to a
                configuration file, or a list of paths to multiple configuration
                files.
            parse_env_config: Whether to look for configuration values in environment
                variables.
            validate_config: True to require validation of config settings.
            setup_mapper: True to setup the mapper. Set to False if you want to
            message_reader: The message reader to use for reading messages.
        """
        super().__init__(
            config=config,
            parse_env_config=parse_env_config,
            validate_config=validate_config,
            message_reader=message_reader,
        )

        self._latest_state: dict[str, dict] = {}
        self._drained_state: dict[str, dict] = {}
        self._sinks_active: dict[str, Sink] = {}
        self._sinks_to_clear: list[Sink] = []
        self._max_parallelism: int | None = _MAX_PARALLELISM

        # Approximated for max record age enforcement
        self._last_full_drain_at: float = time.time()

        self._mapper: PluginMapper | None = None

        if setup_mapper:
            self.setup_mapper()

    @classproperty
    def capabilities(self) -> list[CapabilitiesEnum]:  # noqa: PLR6301
        """Get target capabilities.

        Returns:
            A list of capabilities supported by this target.
        """
        return [
            PluginCapabilities.ABOUT,
            PluginCapabilities.STREAM_MAPS,
            PluginCapabilities.FLATTENING,
            TargetCapabilities.VALIDATE_RECORDS,
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
        record: dict | None = None,
        schema: dict | None = None,
        key_properties: t.Sequence[str] | None = None,
    ) -> Sink:
        """Return a sink for the given stream name.

        A new sink will be created if `schema` is provided and if either `schema` or
        `key_properties` has changed. If so, the old sink becomes archived and held
        until the next drain_all() operation.

        Developers only need to override this method if they want to provide a different
        sink depending on the values within the `record` object. Otherwise, please see
        `default_sink_class` property and/or the `get_sink_class()` method.

        Raises :class:`singer_sdk.exceptions.RecordsWithoutSchemaException` if sink does
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
            return self._sinks_active[stream_name]

        existing_sink = self._sinks_active.get(stream_name, None)
        if not existing_sink:
            return self.add_sink(stream_name, schema, key_properties)

        if (
            existing_sink.original_schema != schema
            or existing_sink.key_properties != key_properties
        ):
            self.logger.info(
                "Schema or key properties for '%s' stream have changed. "
                "Initializing a new '%s' sink...",
                stream_name,
                stream_name,
            )
            self._sinks_to_clear.append(self._sinks_active.pop(stream_name))
            return self.add_sink(stream_name, schema, key_properties)

        return existing_sink

    def get_sink_class(self, stream_name: str) -> type[Sink]:
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

        msg = (
            f"No sink class defined for '{stream_name}' and no default sink class "
            "available."
        )
        raise ValueError(msg)

    def sink_exists(self, stream_name: str) -> bool:
        """Check sink for a stream.

        This method is internal to the SDK and should not need to be overridden.

        Args:
            stream_name: Name of the stream

        Returns:
            True if a sink has been initialized.
        """
        return stream_name in self._sinks_active

    @t.final
    def add_sink(
        self,
        stream_name: str,
        schema: dict,
        key_properties: t.Sequence[str] | None = None,
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
        self.logger.info("Initializing '%s' target sink...", self.name)
        sink_class = self.get_sink_class(stream_name=stream_name)
        sink = sink_class(
            target=self,
            stream_name=stream_name,
            schema=schema,
            key_properties=key_properties,
        )

        try:
            sink.setup()
        except Exception:  # pragma: no cover
            self.logger.error("Error initializing '%s' target sink", self.name)  # noqa: TRY400
            raise

        self._sinks_active[stream_name] = sink
        return sink

    def _assert_sink_exists(self, stream_name: str) -> None:
        """Raise a RecordsWithoutSchemaException exception if stream doesn't exist.

        Args:
            stream_name: TODO

        Raises:
            RecordsWithoutSchemaException: If sink does not exist and schema
                is not sent.
        """
        if not self.sink_exists(stream_name):
            msg = (
                f"A record for stream '{stream_name}' was encountered before a "
                "corresponding schema. Check that the Tap correctly implements "
                "the Singer spec."
            )
            raise RecordsWithoutSchemaException(msg)

    # Message handling

    def _handle_max_record_age(self) -> None:
        """Check if _MAX_RECORD_AGE_IN_MINUTES reached, and if so trigger drain."""
        if self._max_record_age_in_minutes > self._MAX_RECORD_AGE_IN_MINUTES:
            self.logger.info(
                "One or more records have exceeded the max age of %d minutes. "
                "Draining all sinks.",
                self._MAX_RECORD_AGE_IN_MINUTES,
            )
            self.drain_all()

    def process_endofpipe(self) -> None:
        """Called after all input lines have been read."""
        self.drain_all(is_endofpipe=True)

    def _process_record_message(self, message_dict: dict) -> None:
        """Process a RECORD message.

        Args:
            message_dict: TODO
        """
        self._assert_line_requires(message_dict, requires={"stream", "record"})

        stream_name = message_dict["stream"]
        if stream_name not in self.mapper.stream_maps:
            self._assert_sink_exists(stream_name)

        for stream_map in self.mapper.stream_maps[stream_name]:
            raw_record = copy.copy(message_dict["record"])
            transformed_record = stream_map.transform(raw_record)
            if transformed_record is None:
                # Record was filtered out by the map transform
                continue

            self._assert_sink_exists(stream_map.stream_alias)
            sink = self.get_sink(stream_map.stream_alias, record=transformed_record)
            context = sink._get_context(transformed_record)  # noqa: SLF001
            if sink.include_sdc_metadata_properties:
                sink._add_sdc_metadata_to_record(  # noqa: SLF001
                    transformed_record,
                    message_dict,
                    context,
                )
            else:
                sink._remove_sdc_metadata_from_record(transformed_record)  # noqa: SLF001

            sink._validate_and_parse(transformed_record)  # noqa: SLF001
            transformed_record = sink.preprocess_record(transformed_record, context)
            sink._singer_validate_message(transformed_record)  # noqa: SLF001

            sink.tally_record_read()
            sink.process_record(transformed_record, context)
            sink.record_counter_metric.increment()
            sink._after_process_record(context)  # noqa: SLF001

            if sink.is_full:
                self.logger.info(
                    "Target sink for '%s' is full. Current size is '%s'. Draining...",
                    sink.stream_name,
                    sink.current_size,
                )
                self.drain_one(sink)

        self._handle_max_record_age()

    def _process_schema_message(self, message_dict: dict) -> None:
        """Process a SCHEMA messages.

        Args:
            message_dict: The newly received schema message.
        """
        self._assert_line_requires(message_dict, requires={"stream", "schema"})
        self._assert_line_requires(message_dict["schema"], requires={"properties"})

        stream_name = message_dict["stream"]
        schema = message_dict["schema"]
        key_properties = message_dict.get("key_properties")
        do_registration = False
        if stream_name not in self.mapper.stream_maps:
            do_registration = True
        elif self.mapper.stream_maps[stream_name][0].raw_schema != schema:
            self.logger.info(
                "Schema has changed for stream '%s'. "
                "Mapping definitions will be reset.",
                stream_name,
            )
            do_registration = True
        elif (
            self.mapper.stream_maps[stream_name][0].raw_key_properties != key_properties
        ):
            self.logger.info(
                "Key properties have changed for stream '%s'. "
                "Mapping definitions will be reset.",
                stream_name,
            )
            do_registration = True

        if not do_registration:
            self.logger.debug(
                "No changes detected in SCHEMA message for stream '%s'. Ignoring.",
                stream_name,
            )
            return

        self.mapper.register_raw_stream_schema(
            stream_name,
            schema,
            key_properties,
        )
        for stream_map in self.mapper.stream_maps[stream_name]:
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
        self._assert_line_requires(message_dict, requires={"value"})
        state = message_dict["value"]
        if self._latest_state == state:
            return
        self._latest_state = state

    def _process_activate_version_message(self, message_dict: dict) -> None:
        """Handle the optional ACTIVATE_VERSION message extension.

        Args:
            message_dict: TODO
        """
        stream_name = message_dict["stream"]

        for stream_map in self.mapper.stream_maps[stream_name]:
            sink = self.get_sink(stream_map.stream_alias)
            if not sink.process_activate_version_messages:
                self.logger.warning(
                    "`ACTIVATE_VERSION` messages are not enabled for '%s'. Ignoring.",
                    stream_map.stream_alias,
                )
                continue
            if not sink.include_sdc_metadata_properties:
                self.logger.warning(
                    "The `ACTIVATE_VERSION` feature uses the `_sdc_deleted_at` and "
                    "`_sdc_deleted_at` metadata properties so they will be added to "
                    "the schema for '%s' even though `add_record_metadata` is "
                    "disabled.",
                )
            sink.activate_version(message_dict["version"])

    def _process_batch_message(self, message_dict: dict) -> None:
        """Handle the optional BATCH message extension.

        Args:
            message_dict: TODO
        """
        stream_name = message_dict["stream"]
        encoding = BaseBatchFileEncoding.from_dict(message_dict["encoding"])

        for stream_map in self.mapper.stream_maps[stream_name]:
            sink = self.get_sink(stream_map.stream_alias)
            sink.process_batch_files(
                encoding,
                message_dict["manifest"],
            )
            self._handle_max_record_age()

    # Sink drain methods

    @t.final
    def drain_all(self, *, is_endofpipe: bool = False) -> None:
        """Drains all sinks, starting with those cleared due to changed schema.

        This method is internal to the SDK and should not need to be overridden.

        Args:
            is_endofpipe: This is called after the target instance has finished
                listening to the stdin.
        """
        state = copy.deepcopy(self._latest_state)
        self._drain_all(self._sinks_to_clear, 1)
        if is_endofpipe:
            for sink in self._sinks_to_clear:
                sink.clean_up()
        self._sinks_to_clear = []
        self._drain_all(list(self._sinks_active.values()), self.max_parallelism)
        if is_endofpipe:
            for sink in self._sinks_active.values():
                sink.clean_up()
        self._write_state_message(state)
        self._reset_max_record_age()

    @t.final
    def drain_one(self, sink: Sink) -> None:  # noqa: PLR6301
        """Drain a specific sink.

        This method is internal to the SDK and should not need to be overridden.

        Args:
            sink: Sink to be drained.
        """
        if sink.current_size == 0:
            return

        draining_status = sink.start_drain()
        with sink.batch_processing_timer:
            sink.process_batch(draining_status)
        sink.mark_drained()

    def _drain_all(self, sink_list: list[Sink], parallelism: int) -> None:
        if parallelism == 1:
            for sink in sink_list:
                self.drain_one(sink)
            return

        def _drain_sink(sink: Sink) -> None:
            self.drain_one(sink)

        with parallel_config(backend="threading", n_jobs=parallelism):
            Parallel()(delayed(_drain_sink)(sink=sink) for sink in sink_list)

    def _write_state_message(self, state: dict) -> None:
        """Emit the stream's latest state.

        Args:
            state: TODO
        """
        state_json = json.dumps(state)
        self.logger.info("Emitting completed target state %s", state_json)
        sys.stdout.write(f"{state_json}\n")
        sys.stdout.flush()

    # CLI handler

    @classmethod
    def invoke(  # type: ignore[override]
        cls: type[Target],
        *,
        about: bool = False,
        about_format: str | None = None,
        config: tuple[str, ...] = (),
        file_input: t.IO[str] | None = None,
    ) -> None:
        """Invoke the target.

        Args:
            about: Display package metadata and settings.
            about_format: Specify output style for `--about`.
            config: Configuration file location or 'ENV' to use environment
                variables. Accepts multiple inputs as a tuple.
            file_input: Optional file to read input from.
        """
        super().invoke(about=about, about_format=about_format)
        cls.print_version(print_fn=cls.logger.info)
        config_files, parse_env_config = cls.config_from_cli_args(*config)

        target = cls(
            config=config_files,  # type: ignore[arg-type]
            validate_config=True,
            parse_env_config=parse_env_config,
        )
        target.listen(file_input)

    @classmethod
    def get_singer_command(cls: type[Target]) -> click.Command:
        """Execute standard CLI handler for taps.

        Returns:
            A click.Command object.
        """
        command = super().get_singer_command()
        command.help = "Execute the Singer target."
        command.params.extend(
            [
                click.Option(
                    ["--input", "file_input"],
                    help="A path to read messages from instead of from standard in.",
                    type=click.File("r"),
                ),
            ],
        )

        return command

    @classmethod
    def append_builtin_config(cls: type[Target], config_jsonschema: dict) -> None:
        """Appends built-in config to `config_jsonschema` if not already set.

        To customize or disable this behavior, developers may either override this class
        method or override the `capabilities` property to disabled any unwanted
        built-in capabilities.

        For all except very advanced use cases, we recommend leaving these
        implementations "as-is", since this provides the most choice to users and is
        the most "future proof" in terms of taking advantage of built-in capabilities
        which may be added in the future.

        Args:
            config_jsonschema: [description]
        """

        def _merge_missing(source_jsonschema: dict, target_jsonschema: dict) -> None:
            # Append any missing properties in the target with those from source.
            for k, v in source_jsonschema["properties"].items():
                if k not in target_jsonschema["properties"]:
                    target_jsonschema["properties"][k] = v

        _merge_missing(ADD_RECORD_METADATA_CONFIG, config_jsonschema)
        _merge_missing(TARGET_LOAD_METHOD_CONFIG, config_jsonschema)
        _merge_missing(TARGET_BATCH_SIZE_ROWS_CONFIG, config_jsonschema)

        capabilities = cls.capabilities

        if PluginCapabilities.ACTIVATE_VERSION in capabilities:
            _merge_missing(ACTIVATE_VERSION_CONFIG, config_jsonschema)

        if PluginCapabilities.BATCH in capabilities:
            _merge_missing(BATCH_CONFIG, config_jsonschema)

        if TargetCapabilities.VALIDATE_RECORDS in capabilities:
            _merge_missing(TARGET_VALIDATE_RECORDS_CONFIG, config_jsonschema)

        super().append_builtin_config(config_jsonschema)


class SQLTarget(Target):
    """Target implementation for SQL destinations."""

    _target_connector: SQLConnector | None = None

    default_sink_class: type[SQLSink]

    @property
    def target_connector(self) -> SQLConnector:
        """The connector object.

        Returns:
            The connector object.
        """
        if self._target_connector is None:
            self._target_connector = self.default_sink_class.connector_class(
                dict(self.config),
            )
        return self._target_connector

    @classproperty
    def capabilities(self) -> list[CapabilitiesEnum]:
        """Get target capabilities.

        Returns:
            A list of capabilities supported by this target.
        """
        sql_target_capabilities: list[CapabilitiesEnum] = super().capabilities
        sql_target_capabilities.extend(
            [
                PluginCapabilities.ACTIVATE_VERSION,
                TargetCapabilities.TARGET_SCHEMA,
                TargetCapabilities.HARD_DELETE,
            ]
        )

        return sql_target_capabilities

    @classmethod
    def append_builtin_config(cls: type[SQLTarget], config_jsonschema: dict) -> None:
        """Appends built-in config to `config_jsonschema` if not already set.

        To customize or disable this behavior, developers may either override this class
        method or override the `capabilities` property to disabled any unwanted
        built-in capabilities.

        For all except very advanced use cases, we recommend leaving these
        implementations "as-is", since this provides the most choice to users and is
        the most "future proof" in terms of taking advantage of built-in capabilities
        which may be added in the future.

        Args:
            config_jsonschema: [description]
        """

        def _merge_missing(source_jsonschema: dict, target_jsonschema: dict) -> None:
            # Append any missing properties in the target with those from source.
            for k, v in source_jsonschema["properties"].items():
                if k not in target_jsonschema["properties"]:
                    target_jsonschema["properties"][k] = v

        capabilities = cls.capabilities

        if TargetCapabilities.TARGET_SCHEMA in capabilities:
            _merge_missing(TARGET_SCHEMA_CONFIG, config_jsonschema)

        if TargetCapabilities.HARD_DELETE in capabilities:
            _merge_missing(TARGET_HARD_DELETE_CONFIG, config_jsonschema)

        super().append_builtin_config(config_jsonschema)

    @t.final
    def add_sqlsink(
        self,
        stream_name: str,
        schema: dict,
        key_properties: t.Sequence[str] | None = None,
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
        self.logger.info("Initializing '%s' target sink...", self.name)
        sink_class = self.get_sink_class(stream_name=stream_name)
        sink = sink_class(
            target=self,
            stream_name=stream_name,
            schema=schema,
            key_properties=key_properties,
            connector=self.target_connector,
        )
        sink.setup()
        self._sinks_active[stream_name] = sink

        return sink

    def get_sink_class(self, stream_name: str) -> type[SQLSink]:
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

        msg = (
            f"No sink class defined for '{stream_name}' and no default sink class "
            "available."
        )
        raise ValueError(msg)

    def get_sink(
        self,
        stream_name: str,
        *,
        record: dict | None = None,
        schema: dict | None = None,
        key_properties: t.Sequence[str] | None = None,
    ) -> Sink:
        """Return a sink for the given stream name.

        A new sink will be created if `schema` is provided and if either `schema` or
        `key_properties` has changed. If so, the old sink becomes archived and held
        until the next drain_all() operation.

        Developers only need to override this method if they want to provide a different
        sink depending on the values within the `record` object. Otherwise, please see
        `default_sink_class` property and/or the `get_sink_class()` method.

        Raises :class:`singer_sdk.exceptions.RecordsWithoutSchemaException` if sink does
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
            return self.add_sqlsink(stream_name, schema, key_properties)

        if (
            existing_sink.schema != schema
            or existing_sink.key_properties != key_properties
        ):
            self.logger.info(
                "Schema or key properties for '%s' stream have changed. "
                "Initializing a new '%s' sink...",
                stream_name,
                stream_name,
            )
            self._sinks_to_clear.append(self._sinks_active.pop(stream_name))
            return self.add_sqlsink(stream_name, schema, key_properties)

        return existing_sink
