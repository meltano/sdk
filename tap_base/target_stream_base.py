"""Abstract base class for loading a single singer stream to its target."""

import abc
import datetime
from decimal import Decimal
import json
import sys
import re
from logging import Logger
import collections
from typing import Dict, Iterable, List, Optional

from jsonschema import Draft4Validator, FormatChecker

from dateutil import parser
from dateutil.parser import ParserError

import itertools
import inflection

from tap_base.connection_base import ConnectionBase
from tap_base.tap_stream_base import TapStreamBase


class RecordFlattener:
    """Flattens hierarchical records into 2-dimensional ones."""

    sep: str
    max_level: Optional[int]

    def __init__(self, sep: str = "__", max_level: int = None):
        self.sep = sep
        self.max_level = max_level

    def flatten_key(self, k, parent_key):
        full_key = parent_key + [k]
        inflected_key = full_key.copy()
        reducer_index = 0
        while len(self.sep.join(inflected_key)) >= 255 and reducer_index < len(
            inflected_key
        ):
            reduced_key = re.sub(
                r"[a-z]", "", inflection.camelize(inflected_key[reducer_index])
            )
            inflected_key[reducer_index] = (
                reduced_key
                if len(reduced_key) > 1
                else inflected_key[reducer_index][0:3]
            ).lower()
            reducer_index += 1
        return self.sep.join(inflected_key)

    def flatten_record(self, d, flatten_schema=None, parent_key=[], level=0):
        items = []
        for k, v in d.items():
            new_key = self._flatten_key(k, parent_key)
            if isinstance(v, collections.MutableMapping) and level < self.max_level:
                items.extend(
                    self._flatten_record(
                        v,
                        flatten_schema,
                        parent_key + [k],
                        sep=self.sep,
                        level=level + 1,
                    ).items()
                )
            else:
                items.append(
                    (
                        new_key,
                        json.dumps(v)
                        if self._should_json_dump_value(k, v, flatten_schema)
                        else v,
                    )
                )
        return dict(items)

    @staticmethod
    def _should_json_dump_value(key, value, flatten_schema=None) -> bool:
        if isinstance(value, (dict, list)):
            return True
        if (
            flatten_schema
            and key in flatten_schema
            and "type" in flatten_schema[key]
            and set(flatten_schema[key]["type"]) == {"null", "object", "array"}
        ):
            return True
        return False


class TargetStreamBase(TapStreamBase, metaclass=abc.ABCMeta):

    DEFAULT_BATCH_SIZE_ROWS = 100000
    DEFAULT_PARALLELISM = 0  # 0 The number of threads used to flush tables
    DEFAULT_MAX_PARALLELISM = 16  # Don't use more than this number of threads by default when flushing streams in parallel

    APPEND_SDC_METADATA_COLS = True

    DATETIME_ERROR_TREATMENT = "MAX"
    MAX_FLATTEN_DEPTH = 0

    # max timestamp/datetime supported, used to reset all invalid dates that are beyond this value
    MAX_TIMESTAMP = "9999-12-31 23:59:59.999999"
    MAX_TIME = "23:59:59.999999"

    logger: Logger
    schema: Dict
    stream_name: str
    validator: Draft4Validator
    flattener: Optional[RecordFlattener]

    # TODO: Evaluate whether to keep PK-dedupe algorithm or switch to list/queue.
    _records_cache: Dict[str, Dict] = {}

    _num_records_cached: int = 0
    _total_records_read: int = 0
    _dupe_records_received: int = 0
    _flushed_state: dict = {}
    _cache_state: dict = {}

    def __init__(
        self,
        stream_name: str,
        connection: ConnectionBase,
        schema: Dict,
        logger: Logger,
    ) -> None:
        super().__init__(
            tap_stream_id=stream_name,
            connection=connection,
            catalog_entry=None,
            state=None,
            logger=logger,
        )
        self.schema = schema
        self.stream_name = stream_name
        self.flattener = RecordFlattener(max_level=self.MAX_FLATTEN_DEPTH)
        self.validator = Draft4Validator(schema, format_checker=FormatChecker())

    def process_record(self, record: Dict, message: Dict):
        self.validate_record(record)
        primary_key_string = self.record_primary_key_string(record)
        if not primary_key_string:
            primary_key_string = "RID-{}".format(self._num_records_cached)
        # increment row count only when a new PK is encountered in the current batch
        if primary_key_string not in self._records_cache:
            self._num_records_cached += 1
            self._total_records_read += 1
        else:
            self._dupe_records_received += 1
        if self.APPEND_SDC_METADATA_COLS:
            record = self.add_metadata_values_to_record(record, message)
        self._records_cache[primary_key_string] = record

    @staticmethod
    def add_metadata_values_to_record(record: dict, message: dict) -> Dict:
        """Populate metadata _sdc columns from incoming record message.

        The location of the required attributes are fixed in the stream.
        """
        record["_sdc_extracted_at"] = message.get("time_extracted")
        record["_sdc_batched_at"] = datetime.datetime.now().isoformat()
        record["_sdc_deleted_at"] = record.get("_sdc_deleted_at")
        return record

    def record_primary_key_string(self, record):
        if len(self.stream_schema_message["key_properties"]) == 0:
            return None
        if self.flattener:
            flattened = self.flattener.flatten_record(
                record, self.flatten_schema, max_level=self.data_flattening_max_level
            )
            try:
                key_props = [
                    str(flattened[p])
                    for p in self.stream_schema_message["key_properties"]
                ]
            except Exception as exc:
                self.logger.error(
                    "Cannot find {} primary key(s) in record: {}".format(
                        self.stream_schema_message["key_properties"], flattened
                    )
                )
                raise exc
            return ",".join(key_props)

    def emit_state(self):
        if self._flushed_state:
            line = json.dumps(self._flushed_state)
            self.logger.info(f"Emitting state {line}")
            sys.stdout.write(f"{line}\n")
            sys.stdout.flush()

    @staticmethod
    def _float_to_decimal(value):
        """Walk the given data structure and turn all instances of float into double."""
        if isinstance(value, float):
            return Decimal(str(value))
        if isinstance(value, list):
            return [TargetStreamBase._float_to_decimal(child) for child in value]
        if isinstance(value, dict):
            return {k: TargetStreamBase._float_to_decimal(v) for k, v in value.items()}
        return value

    def _get_datelike_property_type(
        self, property_key: str, property_schema: Dict
    ) -> Optional[str]:
        """Return one of 'date-time', 'time', or 'date' if property is date-like.

        Otherwise return None.
        """
        if "anyOf" in property_schema:
            for type_dict in property_schema["anyOf"]:
                if "string" in type_dict["type"] and type_dict.get("format", None) in {
                    "date-time",
                    "time",
                    "date",
                }:
                    return type_dict["format"]
        if "string" in property_schema["type"] and property_schema.get(
            "format", None
        ) in {"date-time", "time", "date"}:
            return property_schema["format"]
        return None

    def validate_record(self, record: Dict) -> Dict:
        self.validate_timestamps_in_record(
            record=record, schema=self.schema, treatment="null"
        )
        return record

    def validate_timestamps_in_record(
        self, record: Dict, schema: Dict, treatment: str
    ) -> None:
        """
        Goes through every field that is of type date/datetime/time and if its value is out of range,
        resets it to MAX value accordingly
        Args:
            record: record containing properties and values
            schema: json schema that has types of each property
        """
        # creating this internal function to avoid duplicating code and too many nested blocks.
        def reset_new_value(record: Dict, key: str, format: str):
            try:
                parser.parse(record[key])
            except Exception as ex1:
                msg = f"Could not parse value '{record[key]}' for field '{key}'."
                if treatment is None or treatment.lower() == "error":
                    raise ValueError(msg)
                if treatment.lower() == "max":
                    self.logger.warning(f"{msg}. Replacing with MAX value.\n{ex1}\n")
                    record[key] = (
                        self.MAX_TIMESTAMP if format != "time" else self.MAX_TIME
                    )
                    return
                if treatment.lower() == "null":
                    self.logger.warning(f"{msg}. Replacing with NULL.\n{ex1}\n")
                    return

        for key in record.keys():
            datelike_type = self._get_datelike_property_type(
                key, schema["properties"][key]
            )
            if datelike_type:
                reset_new_value(record, key, datelike_type)

    def flush_all(self) -> dict:
        """Call 'flush_records' to save all pending records.

        Returns the latest state.
        """
        # NOTE: This may not be thread safe; same stream should not be processed in
        #       parallel without proper locking.
        new_state, records, num_records = (
            self._cache_state,
            self._records_cache,
            self._num_records_cached,
        )
        self._records_cache, self._num_records_cached = [], 0
        self.flush_records(records, num_records)
        self._flushed_state = new_state
        self.emit_state()
        return self._flushed_state

    @abc.abstractmethod
    def flush_records(
        self, records_to_load: Iterable[Dict], expected_row_count: Optional[int]
    ) -> dict:
        """Abstract method which deals with flushing queued records to the target.

        Returns the latest state.
        """
