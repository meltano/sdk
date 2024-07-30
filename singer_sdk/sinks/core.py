"""Sink classes load data to a target."""

from __future__ import annotations

import abc
import copy
import datetime
import importlib.util
import time
import typing as t
from functools import cached_property
from gzip import GzipFile
from gzip import open as gzip_open
from types import MappingProxyType

import jsonschema
from typing_extensions import override

from singer_sdk._singerlib.json import deserialize_json
from singer_sdk.exceptions import (
    InvalidJSONSchema,
    InvalidRecord,
    MissingKeyPropertiesError,
)
from singer_sdk.helpers._batch import (
    BaseBatchFileEncoding,
    BatchConfig,
    BatchFileFormat,
    StorageTarget,
)
from singer_sdk.helpers._compat import (
    date_fromisoformat,
    datetime_fromisoformat,
    time_fromisoformat,
)
from singer_sdk.helpers._typing import (
    DatetimeErrorTreatmentEnum,
    get_datelike_property_type,
    handle_invalid_timestamp_in_record,
)

if t.TYPE_CHECKING:
    from logging import Logger

    from singer_sdk.target_base import Target


class BaseJSONSchemaValidator(abc.ABC):
    """Abstract base class for JSONSchema validator."""

    def __init__(self, schema: dict[str, t.Any]) -> None:
        """Initialize the record validator.

        Args:
            schema: Schema of the stream to sink.
        """
        self.schema = schema

    @abc.abstractmethod
    def validate(self, record: dict[str, t.Any]) -> None:
        """Validate a record message.

        This method MUST raise an ``InvalidRecord`` exception if the record is invalid.

        Args:
            record: Record message to validate.
        """


class JSONSchemaValidator(BaseJSONSchemaValidator):
    """Validate records using the ``fastjsonschema`` library."""

    def __init__(
        self,
        schema: dict,
        *,
        validate_formats: bool = False,
        format_checker: jsonschema.FormatChecker | None = None,
    ):
        """Initialize the validator.

        Args:
            schema: Schema of the stream to sink.
            validate_formats: Whether JSON string formats (e.g. ``date-time``) should
                be validated.
            format_checker: User-defined format checker.

        Raises:
            InvalidJSONSchema: If the schema provided from tap or mapper is invalid.
        """
        jsonschema_validator = jsonschema.Draft7Validator

        super().__init__(schema)
        if validate_formats:
            format_checker = format_checker or jsonschema_validator.FORMAT_CHECKER
        else:
            format_checker = jsonschema.FormatChecker(formats=())

        try:
            jsonschema_validator.check_schema(schema)
        except jsonschema.SchemaError as e:
            error_message = f"Schema Validation Error: {e}"
            raise InvalidJSONSchema(error_message) from e

        self.validator = jsonschema_validator(
            schema=schema,
            format_checker=format_checker,
        )

    @override
    def validate(self, record: dict):  # noqa: ANN201
        """Validate a record message.

        Args:
            record: Record message to validate.

        Raises:
            InvalidRecord: If the record is invalid.
        """
        try:
            self.validator.validate(record)
        except jsonschema.ValidationError as e:
            raise InvalidRecord(e.message, record) from e


class Sink(metaclass=abc.ABCMeta):  # noqa: PLR0904
    """Abstract base class for target sinks."""

    # max timestamp/datetime supported, used to reset invalid dates

    logger: Logger

    MAX_SIZE_DEFAULT = 10000

    validate_field_string_format = False
    """Enable JSON schema format validation, for example `date-time` string fields."""

    fail_on_record_validation_exception: bool = True
    """Interrupt the target execution when a record fails schema validation."""

    def __init__(
        self,
        target: Target,
        stream_name: str,
        schema: dict,
        key_properties: t.Sequence[str] | None,
    ) -> None:
        """Initialize target sink.

        Args:
            target: Target instance.
            stream_name: Name of the stream to sink.
            schema: Schema of the stream to sink.
            key_properties: Primary key of the stream to sink.
        """
        self.logger = target.logger.getChild(stream_name)
        self.sync_started_at = target.initialized_at
        self._config = dict(target.config)
        self._pending_batch: dict | None = None
        self.stream_name = stream_name
        self.logger.info(
            "Initializing target sink for stream '%s'...",
            stream_name,
        )
        self.original_schema = copy.deepcopy(schema)
        self.schema = schema
        if self.include_sdc_metadata_properties:
            self._add_sdc_metadata_to_schema()
        else:
            self._remove_sdc_metadata_from_schema()
        self.records_to_drain: list[dict] | t.Any = []
        self._context_draining: dict | None = None
        self.latest_state: dict | None = None
        self._draining_state: dict | None = None
        self.drained_state: dict | None = None
        self._key_properties = key_properties or []

        # Tally counters
        self._total_records_written: int = 0
        self._total_dupe_records_merged: int = 0
        self._total_records_read: int = 0
        self._batch_records_read: int = 0
        self._batch_dupe_records_merged: int = 0

        # Batch full markers
        self._batch_size_rows: int | None = target.config.get(
            "batch_size_rows",
        )

        self._validator: BaseJSONSchemaValidator | None = self.get_validator()

    @cached_property
    def validate_schema(self) -> bool:
        """Enable JSON schema record validation.

        Returns:
            True if JSON schema validation is enabled.
        """
        return self.config.get("validate_records", True)

    def get_validator(self) -> BaseJSONSchemaValidator | None:
        """Get a record validator for this sink.

        Override this method to use a custom format validator, or disable record
        validation by returning `None`.

        Returns:
            An instance of a subclass of ``BaseJSONSchemaValidator``.

        Example implementation using the `fastjsonschema`_ library:

        .. code-block:: python

           import fastjsonschema


           class FastJSONSchemaValidator(BaseJSONSchemaValidator):
               def __init__(self, schema: dict[str, t.Any]) -> None:
                   super().__init__(schema)
                   try:
                       self.validator = fastjsonschema.compile(self.schema)
                   except fastjsonschema.JsonSchemaDefinitionException as e:
                       error_message = "Schema Validation Error"
                       raise InvalidJSONSchema(error_message) from e

               def validate(self, record: dict):
                   try:
                       self.validator(record)
                   except fastjsonschema.JsonSchemaValueException as e:
                       error_message = f"Record Message Validation Error: {e.message}"
                       raise InvalidRecord(error_message, record) from e

        .. _fastjsonschema: https://pypi.org/project/fastjsonschema/
        """
        if self.validate_schema:
            return JSONSchemaValidator(
                self.schema,
                validate_formats=self.validate_field_string_format,
            )
        return None

    def _get_context(self, record: dict) -> dict:  # noqa: ARG002, PLR6301
        """Return an empty dictionary by default.

        NOTE: Future versions of the SDK may expand the available context attributes.

        Args:
            record: Individual record in the stream.

        Returns:
            TODO
        """
        return {}

    # Size properties

    @property
    def current_size(self) -> int:
        """Get current batch size.

        Returns:
            The number of records to drain.
        """
        return self._batch_records_read

    @property
    def is_full(self) -> bool:
        """Check against the batch size limit.

        Returns:
            True if the sink needs to be drained.
        """
        return self.current_size >= self.max_size

    @property
    def batch_size_rows(self) -> int | None:
        """The maximum number of rows a batch can accumulate before being processed.

        Returns:
            The max number of rows or None if not set.
        """
        return self._batch_size_rows

    @property
    def max_size(self) -> int:
        """Get max batch size.

        Returns:
            Max number of records to batch before `is_full=True`

        .. versionchanged:: 0.36.0
           This property now takes into account the
           :attr:`~singer_sdk.Sink.batch_size_rows` attribute and the corresponding
           ``batch_size_rows`` target setting.
        """
        return (
            self.batch_size_rows
            if self.batch_size_rows is not None
            else self.MAX_SIZE_DEFAULT
        )

    # Tally methods

    @t.final
    def tally_record_read(self, count: int = 1) -> None:
        """Increment the records read tally.

        This method is called automatically by the SDK when records are read.

        Args:
            count: Number to increase record count by.
        """
        self._total_records_read += count
        self._batch_records_read += count

    @t.final
    def tally_record_written(self, count: int = 1) -> None:
        """Increment the records written tally.

        This method is called automatically by the SDK after
        :meth:`~singer_sdk.Sink.process_record()`
        or :meth:`~singer_sdk.Sink.process_batch()`.

        Args:
            count: Number to increase record count by.
        """
        self._total_records_written += count

    @t.final
    def tally_duplicate_merged(self, count: int = 1) -> None:
        """Increment the records merged tally.

        This method should be called directly by the Target implementation.

        Args:
            count: Number to increase record count by.
        """
        self._total_dupe_records_merged += count
        self._batch_dupe_records_merged += count

    # Properties

    @property
    def config(self) -> t.Mapping[str, t.Any]:
        """Get plugin configuration.

        Returns:
            A frozen (read-only) config dictionary map.
        """
        return MappingProxyType(self._config)

    @property
    def batch_config(self) -> BatchConfig | None:
        """Get batch configuration.

        Returns:
            A frozen (read-only) config dictionary map.
        """
        raw = self.config.get("batch_config")
        return BatchConfig.from_dict(raw) if raw else None

    @property
    def include_sdc_metadata_properties(self) -> bool:
        """Check if metadata columns should be added.

        Returns:
            True if metadata columns should be added.
        """
        return self.config.get("add_record_metadata", False)

    @property
    def datetime_error_treatment(self) -> DatetimeErrorTreatmentEnum:
        """Return a treatment to use for datetime parse errors: ERROR. MAX, or NULL.

        Returns:
            TODO
        """
        return DatetimeErrorTreatmentEnum.ERROR

    @property
    def key_properties(self) -> t.Sequence[str]:
        """Return key properties.

        Override this method to return a list of key properties in a format that is
        compatible with the target.

        Returns:
            A list of stream key properties.
        """
        return self._key_properties

    # Record processing

    def _add_sdc_metadata_to_record(
        self,
        record: dict,
        message: dict,
        context: dict,
    ) -> None:
        """Populate metadata _sdc columns from incoming record message.

        Record metadata specs documented at:
        https://sdk.meltano.com/en/latest/implementation/record_metadata.html

        Args:
            record: Individual record in the stream.
            message: The record message.
            context: Stream partition or context dictionary.
        """
        record["_sdc_extracted_at"] = message.get("time_extracted")
        record["_sdc_received_at"] = datetime.datetime.now(
            tz=datetime.timezone.utc,
        ).isoformat()
        record["_sdc_batched_at"] = (
            context.get("batch_start_time")
            or datetime.datetime.now(tz=datetime.timezone.utc)
        ).isoformat()
        record["_sdc_deleted_at"] = record.get("_sdc_deleted_at")
        record["_sdc_sequence"] = int(round(time.time() * 1000))
        record["_sdc_table_version"] = message.get("version")
        record["_sdc_sync_started_at"] = self.sync_started_at

    def _add_sdc_metadata_to_schema(self) -> None:
        """Add _sdc metadata columns.

        Record metadata specs documented at:
        https://sdk.meltano.com/en/latest/implementation/record_metadata.html
        """
        properties_dict = self.schema["properties"]
        for col in (
            "_sdc_extracted_at",
            "_sdc_received_at",
            "_sdc_batched_at",
            "_sdc_deleted_at",
        ):
            properties_dict[col] = {
                "type": ["null", "string"],
                "format": "date-time",
            }
        for col in ("_sdc_sequence", "_sdc_table_version", "_sdc_sync_started_at"):
            properties_dict[col] = {"type": ["null", "integer"]}

    def _remove_sdc_metadata_from_schema(self) -> None:
        """Remove _sdc metadata columns.

        Record metadata specs documented at:
        https://sdk.meltano.com/en/latest/implementation/record_metadata.html
        """
        properties_dict = self.schema["properties"]
        for col in (
            "_sdc_extracted_at",
            "_sdc_received_at",
            "_sdc_batched_at",
            "_sdc_deleted_at",
            "_sdc_sequence",
            "_sdc_table_version",
            "_sdc_sync_started_at",
        ):
            properties_dict.pop(col, None)

    def _remove_sdc_metadata_from_record(self, record: dict) -> None:  # noqa: PLR6301
        """Remove metadata _sdc columns from incoming record message.

        Record metadata specs documented at:
        https://sdk.meltano.com/en/latest/implementation/record_metadata.html

        Args:
            record: Individual record in the stream.
        """
        record.pop("_sdc_extracted_at", None)
        record.pop("_sdc_received_at", None)
        record.pop("_sdc_batched_at", None)
        record.pop("_sdc_deleted_at", None)
        record.pop("_sdc_sequence", None)
        record.pop("_sdc_table_version", None)
        record.pop("_sdc_sync_started_at", None)

    # Record validation

    def _validate_and_parse(self, record: dict) -> dict:
        """Validate or repair the record, parsing to python-native types as needed.

        Args:
            record: Individual record in the stream.

        Returns:
            TODO

        Raises:
            InvalidRecord: If the record is invalid.
        """
        if self._validator is not None:
            # TODO: Check the performance impact of this try/except block. It runs
            # on every record, so it's probably bad and should be moved up the stack.
            try:
                self._validator.validate(record)
            except InvalidRecord:
                self.logger.exception("Record validation failed")
                if self.fail_on_record_validation_exception:
                    raise

        self._parse_timestamps_in_record(
            record=record,
            schema=self.schema,
            treatment=self.datetime_error_treatment,
        )
        return record

    def _singer_validate_message(self, record: dict) -> None:
        """Ensure record conforms to Singer Spec.

        Args:
            record: Record (after parsing, schema validations and transformations).

        Raises:
            MissingKeyPropertiesError: If record is missing one or more key properties.
        """
        if any(key_property not in record for key_property in self._key_properties):
            msg = (
                f"Record is missing one or more key_properties. \n"
                f"Key Properties: {self._key_properties}, "
                f"Record Keys: {list(record.keys())}"
            )
            raise MissingKeyPropertiesError(
                msg,
            )

    def _parse_timestamps_in_record(
        self,
        record: dict,
        schema: dict,
        treatment: DatetimeErrorTreatmentEnum,
    ) -> None:
        """Parse strings to datetime.datetime values, repairing or erroring on failure.

        Attempts to parse every field that is of type date/datetime/time. If its value
        is out of range, repair logic will be driven by the `treatment` input arg:
        MAX, NULL, or ERROR.

        Args:
            record: Individual record in the stream.
            schema: TODO
            treatment: TODO
        """
        for key, value in record.items():
            if key not in schema["properties"]:
                if value is not None:
                    self.logger.warning("No schema for record field '%s'", key)
                continue
            datelike_type = get_datelike_property_type(schema["properties"][key])
            if datelike_type:
                date_val = value
                try:
                    if value is not None:
                        if datelike_type == "time":
                            date_val = time_fromisoformat(date_val)
                        elif datelike_type == "date":
                            date_val = date_fromisoformat(date_val)
                        else:
                            date_val = datetime_fromisoformat(date_val)
                except ValueError as ex:
                    date_val = handle_invalid_timestamp_in_record(
                        record,
                        [key],
                        date_val,
                        datelike_type,
                        ex,
                        treatment,
                        self.logger,
                    )
                record[key] = date_val

    def _after_process_record(self, context: dict) -> None:
        """Perform post-processing and record keeping. Internal hook.

        Args:
            context: Stream partition or context dictionary.
        """
        self.logger.debug("Processed record: %s", context)

    # SDK developer overrides:

    def preprocess_record(self, record: dict, context: dict) -> dict:  # noqa: ARG002, PLR6301
        """Process incoming record and return a modified result.

        Args:
            record: Individual record in the stream.
            context: Stream partition or context dictionary.

        Returns:
            A new, processed record.
        """
        return record

    @abc.abstractmethod
    def process_record(self, record: dict, context: dict) -> None:
        """Load the latest record from the stream.

        Implementations may either load to the `context` dict for staging (the
        default behavior for Batch types), or permanently write out to the target.

        Anything appended to :attr:`singer_sdk.Sink.records_to_drain` will be
        automatically passed to
        :meth:`~singer_sdk.Sink.process_batch()` to be permanently written during the
        process_batch operation.

        If duplicates are merged, these can be tracked via
        :meth:`~singer_sdk.Sink.tally_duplicate_merged()`.

        Args:
            record: Individual record in the stream.
            context: Stream partition or context dictionary.
        """

    def start_drain(self) -> dict:
        """Set and return `self._context_draining`.

        Returns:
            TODO
        """
        self._context_draining = self._pending_batch or {}
        self._pending_batch = None
        return self._context_draining

    @abc.abstractmethod
    def process_batch(self, context: dict) -> None:
        """Process all records per the batch's `context` dictionary.

        If duplicates are merged, these can optionally be tracked via
        `tally_duplicate_merged()`.

        Args:
            context: Stream partition or context dictionary.

        Raises:
            NotImplementedError: If derived class does not override this method.
        """
        msg = "No handling exists for process_batch()."
        raise NotImplementedError(msg)

    def mark_drained(self) -> None:
        """Reset `records_to_drain` and any other tracking."""
        self.drained_state = self._draining_state
        self._draining_state = None
        self._context_draining = None
        if self._batch_records_read:
            self.tally_record_written(
                self._batch_records_read - self._batch_dupe_records_merged,
            )
        self._batch_records_read = 0

    def activate_version(self, new_version: int) -> None:
        """Bump the active version of the target table.

        This method should be overridden by developers if a custom implementation is
        expected.

        Args:
            new_version: The version number to activate.
        """
        _ = new_version
        self.logger.warning(
            "ACTIVATE_VERSION message received but not implemented by this target. "
            "Ignoring.",
        )

    def setup(self) -> None:
        """Perform any setup actions at the beginning of a Stream.

        Setup is executed once per Sink instance, after instantiation. If a Schema
        change is detected, a new Sink is instantiated and this method is called again.
        """
        self.logger.info("Setting up %s", self.stream_name)

    def clean_up(self) -> None:
        """Perform any clean up actions required at end of a stream.

        Implementations should ensure that clean up does not affect resources
        that may be in use from other instances of the same sink. Stream name alone
        should not be relied on, it's recommended to use a uuid as well.
        """
        self.logger.info("Cleaning up %s", self.stream_name)

    def process_batch_files(
        self,
        encoding: BaseBatchFileEncoding,
        files: t.Sequence[str],
    ) -> None:
        """Process a batch file with the given batch context.

        Args:
            encoding: The batch file encoding.
            files: The batch files to process.

        Raises:
            NotImplementedError: If the batch file encoding is not supported.
        """
        file: GzipFile | t.IO
        storage: StorageTarget | None = None

        for path in files:
            head, tail = StorageTarget.split_url(path)

            if self.batch_config:
                storage = self.batch_config.storage
            else:
                storage = StorageTarget.from_url(head)

            if encoding.format == BatchFileFormat.JSONL:
                with storage.fs(create=False) as batch_fs, batch_fs.open(
                    tail,
                    mode="rb",
                ) as file:
                    context_file = (
                        gzip_open(file) if encoding.compression == "gzip" else file
                    )
                    context = {
                        "records": [deserialize_json(line) for line in context_file]  # type: ignore[attr-defined]
                    }
                    self.process_batch(context)
            elif (
                importlib.util.find_spec("pyarrow")
                and encoding.format == BatchFileFormat.PARQUET
            ):
                import pyarrow.parquet as pq  # noqa: PLC0415

                with storage.fs(create=False) as batch_fs, batch_fs.open(
                    tail,
                    mode="rb",
                ) as file:
                    context_file = file
                    table = pq.read_table(context_file)
                    context = {"records": table.to_pylist()}
                    self.process_batch(context)
            else:
                msg = f"Unsupported batch encoding format: {encoding.format}"
                raise NotImplementedError(msg)
