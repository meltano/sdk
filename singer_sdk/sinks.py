"""Sink classes load data to a target."""

import abc
import datetime
import time

from logging import Logger
from types import MappingProxyType
from typing import Dict, Optional, List, Any, Mapping, Union
import uuid

from singer_sdk.helpers._compat import final

from jsonschema import Draft4Validator, FormatChecker

# TODO: Re-implement schema validation
# from singer_sdk.helpers._flattening import RecordFlattener

from singer_sdk.helpers._typing import (
    get_datelike_property_type,
    handle_invalid_timestamp_in_record,
    DatetimeErrorTreatmentEnum,
)
from singer_sdk.plugin_base import PluginBase

from dateutil import parser

JSONSchemaValidator = Draft4Validator


class Sink(metaclass=abc.ABCMeta):
    """Abstract base class for target sinks."""

    # max timestamp/datetime supported, used to reset invalid dates

    logger: Logger

    MAX_SIZE_DEFAULT = 10000

    # TODO: Re-implement schema flattening
    # _flattener: Optional[RecordFlattener]
    # _MAX_FLATTEN_DEPTH = 0

    def __init__(
        self,
        target: PluginBase,
        stream_name: str,
        schema: Dict,
        key_properties: Optional[List[str]],
    ) -> None:
        """Initialize target sink."""
        self.logger = target.logger
        self._config = dict(target.config)
        self._active_batch: Optional[dict] = None
        self.schema = schema
        self.stream_name = stream_name
        self.logger.info(f"Initializing target sink for stream '{stream_name}'...")
        self.records_to_drain: Union[List[dict], Any] = []
        self._context_draining: Optional[dict] = None
        self.latest_state: Optional[dict] = None
        self._draining_state: Optional[dict] = None
        self.drained_state: Optional[dict] = None
        self.key_properties = key_properties

        # Tally counters
        self._total_records_written: int = 0
        self._total_dupe_records_merged: int = 0
        self._total_records_read: int = 0
        self._batch_records_read: int = 0
        self._batch_dupe_records_merged: int = 0

        self._validator = Draft4Validator(schema, format_checker=FormatChecker())
        # TODO: Re-implement schema flattener
        # self._flattener = RecordFlattener(max_level=self._MAX_FLATTEN_DEPTH)

    def _get_context(self, record: dict) -> dict:
        """Return an empty dictionary by default.

        NOTE: Future versions of the SDK may expand the available context attributes.
        """
        return {}

    # Size properties

    @property
    def max_size(self) -> int:
        """Return the max number of records that can be held before is_full=True."""
        return self.MAX_SIZE_DEFAULT

    @property
    def current_size(self) -> int:
        """Return the number of records to drain."""
        return self._batch_records_read

    @property
    def is_full(self) -> bool:
        """Return True if the sink needs to be drained."""
        return self.current_size >= self.max_size

    # Tally methods

    @final
    def tally_record_read(self, count: int = 1):
        """Increment the records read tally.

        This method is called automatically by the SDK when records are read.
        """
        self._total_records_read += count
        self._batch_records_read += count

    @final
    def tally_record_written(self, count: int = 1):
        """Increment the records written tally.

        This method is called automatically by the SDK after `process_record()`
        or `process_batch()`.
        """
        self._total_records_written += count

    @final
    def tally_duplicate_merged(self, count: int = 1):
        """Increment the records merged tally.

        This method should be called directly by the Target implementation.
        """
        self._total_dupe_records_merged += count
        self._batch_dupe_records_merged += count

    # Properties

    @property
    def config(self) -> Mapping[str, Any]:
        """Return a frozen (read-only) config dictionary map."""
        return MappingProxyType(self._config)

    @property
    def include_sdc_metadata_properties(self) -> bool:
        """Return True if metadata columns should be added."""
        return True

    @property
    def primary_keys_required(self) -> bool:
        """Return True if primary keys are required."""
        return self.config.get("primary_keys_required", False)

    @property
    def datetime_error_treatment(self) -> DatetimeErrorTreatmentEnum:
        """Return a treatment to use for datetime parse errors: ERROR. MAX, or NULL."""
        return DatetimeErrorTreatmentEnum.ERROR

    # Record processing

    def _add_metadata_values_to_record(self, record: dict, message: dict) -> None:
        """Populate metadata _sdc columns from incoming record message."""
        record["_sdc_extracted_at"] = message.get("time_extracted")
        record["_sdc_batched_at"] = datetime.datetime.now().isoformat()
        record["_sdc_deleted_at"] = record.get("_sdc_deleted_at")
        record["_sdc_received_at"] = datetime.datetime.now().isoformat()
        record["_sdc_sequence"] = int(round(time.time() * 1000))
        record["_sdc_table_version"] = message.get("version")
        record["_sdc_primary_key"] = self.key_properties

    def _remove_metadata_values_from_record(self, record: dict) -> None:
        """Remove metadata _sdc columns from incoming record message."""
        record.pop("_sdc_extracted_at", None)
        record.pop("_sdc_batched_at", None)
        record.pop("_sdc_deleted_at", None)
        record.pop("_sdc_received_at", None)
        record.pop("_sdc_sequence", None)
        record.pop("_sdc_table_version", None)
        record.pop("_sdc_primary_key", None)

    # Record validation

    def _validate_record(self, record: Dict) -> Dict:
        """Validate or repair the record."""
        self._validate_timestamps_in_record(
            record=record, schema=self.schema, treatment=self.datetime_error_treatment
        )
        self._validator.validate(record)
        return record

    def _validate_timestamps_in_record(
        self, record: Dict, schema: Dict, treatment: DatetimeErrorTreatmentEnum
    ) -> None:
        """Confirm or repair date or timestamp values in record.

        Goes through every field that is of type date/datetime/time and if its value is
        out of range, send it to self._handle_invalid_timestamp_in_record() and use the
        return value as replacement.
        """
        for key in record.keys():
            datelike_type = get_datelike_property_type(key, schema["properties"][key])
            if datelike_type:
                try:
                    date_val = record[key]
                    date_val = parser.parse(date_val)
                except Exception as ex:
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

    def _after_process_record(self, context) -> None:
        """Perform post-processing and record keeping. Internal hook."""
        pass

    # SDK developer overrides:

    def preprocess_record(self, record: Dict, context: dict) -> dict:
        """Process incoming record and return a modified result."""
        return record

    @abc.abstractmethod
    def process_record(self, record: dict, context: dict) -> None:
        """Load the latest record from the stream.

        Implementations may either load to the `context` dict for staging (the
        default behavior for Batch types), or permanently write out to the target.

        Anything appended to `self.records_to_drain` will be automatically passed to
        `self.process_batch()` to be permanently written during the process_batch
        operation.

        If duplicates are merged, these can be tracked via `tally_duplicates_merged()`.
        """
        pass

    def start_drain(self) -> dict:
        """Set and return `self._context_draining`."""
        self._context_draining = self._active_batch or {}
        return self._context_draining

    @abc.abstractmethod
    def process_batch(self, context: dict) -> None:
        """Process all records per the batch's `context` dictionary.

        If duplicates are merged, these can optionally be tracked via
        `tally_duplicates_merged()`.
        """
        raise NotImplementedError("No handling exists for process_batch().")

    def mark_drained(self) -> None:
        """Reset `records_to_drain` and any other tracking."""
        self.drained_state = self._draining_state
        self._draining_state = None
        self._context_draining = None
        if self._batch_records_read:
            self.tally_record_written(
                self._batch_records_read - self._batch_dupe_records_merged
            )
        self._batch_records_read = 0


class RecordSink(Sink):
    """Base class for singleton record writers."""

    current_size = 0  # Records are always written directly

    def _after_process_record(self, context) -> None:
        """Perform post-processing and record keeping. Internal hook.

        The RecordSink class uses this method to tally each record written.
        """
        self.tally_record_written()

    @final
    def process_batch(self, context: dict) -> None:
        """Do nothing and return immediately.

        The RecordSink class does not support batching.

        This method may not be overridden.
        """
        pass

    @final
    def start_batch(self, context: dict) -> None:
        """Do nothing and return immediately.

        The RecordSink class does not support batching.

        This method may not be overridden.
        """
        pass

    @abc.abstractmethod
    def process_record(self, record: dict, context: dict) -> None:
        """Load the latest record from the stream.

        This method must be overridden.

        Implementations should permanently serialize each record to the target
        prior to returning.

        If duplicates are merged/skipped instead of being loaded, merges can be
        tracked via `tally_duplicates_merged()`.
        """
        pass


class BatchSink(Sink):
    """Base class for batched record writers."""

    def _get_context(self, record: dict) -> dict:
        """Return a batch context. If no batch is active, return a new batch context.

        Batch contexts by default are always created with a single "batch_id"
        containing a unique GUID string.

        NOTE: Future versions of the SDK may expand the available context attributes.
        """
        if self._active_batch is None:
            new_context = {"batch_id": str(uuid.uuid4())}
            self.start_batch(new_context)
            self._active_batch = new_context

        return self._active_batch

    def start_batch(self, context: dict) -> None:
        """Start a new batch with the given context.

        The initial generated context will have a single `batch_id` entry, which
        a, SDK-generated GUID string which uniquely identifies this batch.

        Developers may optionally override this method to add custom markers to the
        `context` dict and/or to initialize batch resources - such as creating a local
        temp file to store records.
        """
        pass

    def process_record(self, record: dict, context: dict) -> None:
        """Load the latest record from the stream.

        Developers may either load to the `context` dict for staging (the
        default behavior for Batch types), or permanently write out to the target.

        If this method is not overridden, the default implementation will create a
        `context["records"]` list and append all records for processing during
        `process_batch()`.

        If duplicates are merged, these can be tracked via `tally_duplicates_merged()`.
        """
        if "records" not in context:
            context["records"] = []

        context["records"].append(record)

    @abc.abstractmethod
    def process_batch(self, context: dict) -> None:
        """Process a batch with the given batch context.

        This method must be overridden.

        If `process_record()` is not overridden, the `context["records"]` list
        will contain all records from the given batch context.

        If duplicates are merged, these can be tracked via `tally_duplicates_merged()`.
        """
        pass
