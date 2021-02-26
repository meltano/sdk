"""Stream abstract class."""

import abc  # abstract base classes
import datetime
import json
import logging
from types import MappingProxyType

import pendulum
from singer import metadata

from singer_sdk.plugin_base import PluginBase as TapBaseClass
from singer_sdk.helpers.secrets import SecretString
from singer_sdk.helpers.util import get_property_schema, is_boolean_type
from singer_sdk.helpers.state import (
    get_stream_state_dict,
    read_stream_state,
    wipe_stream_state_keys,
)
import time
from functools import lru_cache
from os import PathLike
from pathlib import Path
from typing import (
    Dict,
    Any,
    List,
    Iterable,
    Mapping,
    Optional,
    TypeVar,
    Union,
)

try:
    from typing import final
except ImportError:
    # Final not available until Python3.8
    final = lambda f: f  # noqa: E731

import singer
from singer import RecordMessage, SchemaMessage
from singer.catalog import Catalog
from singer.schema import Schema

DEBUG_MODE = True

# How many records to emit between sending state message updates
STATE_MSG_FREQUENCY = 10 if DEBUG_MODE else 10000


FactoryType = TypeVar("FactoryType", bound="Stream")


class Stream(metaclass=abc.ABCMeta):
    """Abstract base class for tap streams."""

    MAX_CONNECT_RETRIES = 0

    parent_stream_types: List[Any] = []  # May be used in sync sequencing

    def __init__(
        self,
        tap: TapBaseClass,
        schema: Optional[Union[str, PathLike, Dict[str, Any], Schema]],
        name: Optional[str],
    ):
        """Init tap stream."""
        if name:
            self.name: str = name
        if not self.name:
            raise ValueError("Missing argument or class variable 'name'.")
        self.logger: logging.Logger = tap.logger
        self.tap_name: str = tap.name
        self._config: dict = dict(tap.config)
        self._tap_state = tap.state
        self.forced_replication_method: Optional[str] = None
        self.replication_key: Optional[str] = None
        self.primary_keys: Optional[List[str]] = None
        self._schema_filepath: Optional[Path] = None
        self._schema: Optional[dict] = None
        if schema:
            if isinstance(schema, (PathLike, str)):
                self._schema_filepath = Path(schema)
            elif isinstance(schema, dict):
                self._schema = schema
            elif isinstance(schema, Schema):
                self._schema = schema.to_dict()
            elif schema:
                raise ValueError(
                    f"Unexpected type {type(schema).__name__} for arg 'schema'."
                )

    @property
    def is_timestamp_replication_key(self) -> bool:
        if not self.replication_key:
            return False
        type_dict = self.schema.get(self.replication_key)
        if isinstance(type_dict, dict) and type_dict.get("format") == "date-time":
            return True
        return False

    def get_starting_datetime(self, partition: Optional[dict]) -> Optional[datetime.datetime]:
        result: Optional[datetime.datetime] = None
        if self.is_timestamp_replication_key:
            state = self.get_stream_or_partition_state(partition)
            if self.replication_key in state:
                result = pendulum.parse(state[self.replication_key])
        if result is None and "start_date" in self.config:
            result = pendulum.parse(self.config.get("start_date"))
        return result

    @property
    def schema_filepath(self) -> Optional[Path]:
        """Return a path to a schema file for the stream or None if n/a."""
        return self._schema_filepath

    @property
    def schema(self) -> dict:
        """Return the schema dict for the stream."""
        if not self._schema:
            if self.schema_filepath:
                if not Path(self.schema_filepath).is_file():
                    raise FileExistsError(
                        f"Could not find schema file '{self.schema_filepath}'."
                    )
                self._schema = json.loads(Path(self.schema_filepath).read_text())
        if not self._schema:
            raise ValueError(
                f"Could not initialize schema for stream '{self.name}'. "
                "A valid schema object or filepath was not provided."
            )
        return self._schema

    @property
    def singer_metadata(self) -> dict:
        self.logger.debug(f"Schema Debug: {self.schema}")
        md = metadata.get_standard_metadata(
            schema=self.schema,
            replication_method=self.replication_method,
            key_properties=self.primary_keys or None,
            valid_replication_keys=(
                [self.replication_key] if self.replication_key else None
            ),
            schema_name=None,
        )
        return md

    @property
    def singer_catalog_entry(self) -> singer.CatalogEntry:
        return singer.CatalogEntry(
            tap_stream_id=self.tap_stream_id,
            stream=self.name,
            schema=Schema.from_dict(self.schema),
            metadata=self.singer_metadata,
            key_properties=self.primary_keys or None,
            replication_key=self.replication_key,
            replication_method=self.replication_method,
            is_view=None,
            database=None,
            table=None,
            row_count=None,
            stream_alias=None,
        )

    @property
    def config(self) -> Mapping[str, Any]:
        """Return a frozen (read-only) config dictionary map."""
        return MappingProxyType(self._config)

    @property
    def tap_stream_id(self) -> str:
        """Return a unique stream ID.

        Default implementations will return `self.name` but this behavior may be
        overridden if required by the developer.
        """
        return self.name

    @property
    def replication_method(self) -> str:
        """Return the replication method to be used."""
        if self.forced_replication_method:
            return str(self.forced_replication_method)
        if self.replication_key:
            return "INCREMENTAL"
        return "FULL_TABLE"

    # State properties:

    @property
    def tap_state(self) -> dict:
        """Return a writeable state dict for the entire tap.

        Note: This dictionary is shared (and writable) across all streams.
        """
        return self._tap_state

    def get_stream_or_partition_state(self, partition: Optional[dict]) -> dict:
        """If partition is provided, return the partition state. Otherwise, return the stream state."""
        if partition:
            return self.get_partition_state(partition)
        return self.stream_state

    @property
    def stream_state(self) -> dict:
        """Return a writeable state dict for this stream.

        A blank state entry will be created if one doesn't already exist.
        """
        return get_stream_state_dict(self.tap_state, self.name)

    def get_partition_state(self, partition: dict) -> dict:
        """Return a writable state dict for the given partition."""
        return get_stream_state_dict(self.tap_state, self.name, partition=partition)

    # Partitions

    @property
    def partitions(self) -> Optional[List[dict]]:
        """Return a list of partition key dicts (if applicable), otherwise None.
        
        Developers may override this property to provide a default partitions list.
        """
        state = read_stream_state(self.tap_state, self.name)
        if state is None or "partitions" not in state:
            return None
        result: List[dict] = []
        for partition_state in state["partitions"]:
            result.append(partition_state.get("context"))
        return result

    # Private bookmarking methods

    def _increment_stream_state(
        self, latest_record: Dict[str, Any], *, partition: Optional[dict] = None
    ):
        """Update state of the stream or partition with data from the provided record."""
        state_dict = self.get_stream_or_partition_state(partition)
        if latest_record:
            if self.replication_method == "FULL_TABLE":
                max_pk_values = self._get_bookmark("max_pk_values")
                if max_pk_values:
                    state_dict["last_pk_fetched"] = {
                        k: v
                        for k, v in latest_record.items()
                        if k in (self.primary_keys or [])
                    }
            elif self.replication_method in ["INCREMENTAL", "LOG_BASED"]:
                if not self.replication_key:
                    raise ValueError(
                        f"Could not detect replication key for '{self.name}' stream"
                        f"(replication method={self.replication_method})"
                    )
                state_dict.update(
                    {
                        "replication_key": self.replication_key,
                        "replication_key_value": latest_record[self.replication_key],
                    }
                )

    def _get_bookmark(self, key: str, default: Any = None):
        """Return a bookmark key's value, or a default value if key is not set."""
        return singer.get_bookmark(
            state=self.tap_state,
            tap_stream_id=self.tap_stream_id,
            key=key,
            default=default,
        )

    # Private message authoring methods:

    def _write_state_message(self):
        """Write out a STATE message with the latest state."""
        singer.write_message(singer.StateMessage(value=self.tap_state))

    def _write_schema_message(self):
        """Write out a SCHEMA message with the stream schema."""
        bookmark_keys = [self.replication_key] if self.replication_key else None
        schema_message = SchemaMessage(
            self.tap_stream_id, self.schema, self.primary_keys, bookmark_keys
        )
        singer.write_message(schema_message)

    # Private sync methods:

    def _sync_records(self, partition: Optional[dict] = None) -> None:
        """Sync records, emitting RECORD and STATE messages."""
        rows_sent = 0
        # Reset interim state keys from prior executions:
        wipe_stream_state_keys(
            self.tap_state,
            self.name,
            except_keys=[
                "last_pk_fetched",
                "max_pk_values",
                "version",
                "initial_full_table_complete",
            ],
        )
        # Iterate through each returned record:
        if partition:
            partitions = [partition]
        else:
            partitions = self.partitions or [None]
        for partition in partitions:
            for row_dict in self.get_records(partition=partition):
                if rows_sent and ((rows_sent - 1) % STATE_MSG_FREQUENCY == 0):
                    self._write_state_message()
                record = self._conform_record_data_types(row_dict)
                record_message = RecordMessage(
                    stream=self.name,
                    record=record,
                    version=None,
                    time_extracted=pendulum.now(),
                )
                singer.write_message(record_message)
                self._increment_stream_state(record, partition=partition)
                rows_sent += 1
        self.logger.info(f"Completed '{self.name}' sync ({rows_sent} records).")
        # Reset interim bookmarks before emitting final STATE message:
        wipe_stream_state_keys(
            self.tap_state, self.name, except_keys=["last_pk_fetched", "max_pk_values"],
        )
        self._write_state_message()

    # Private validation and cleansing methods:

    @lru_cache()
    def _warn_unmapped_property(self, property_name: str):
        self.logger.warning(
            f"Property '{property_name}' was present in the result stream but "
            "not found in catalog schema. Ignoring."
        )

    def _conform_record_data_types(self, row: Dict[str, Any]) -> RecordMessage:
        """Translate values in record dictionary to singer-compatible data types.

        Any property names not found in the schema catalog will be removed, and a
        warning will be logged exactly once per unmapped property name.
        """
        rec: Dict[str, Any] = {}
        for property_name, elem in row.items():
            property_schema = get_property_schema(self.schema or {}, property_name)
            if not property_schema:
                self._warn_unmapped_property(property_name)
                continue
            if isinstance(elem, datetime.datetime):
                rec[property_name] = elem.isoformat() + "+00:00"
            elif isinstance(elem, datetime.date):
                rec[property_name] = elem.isoformat() + "T00:00:00+00:00"
            elif isinstance(elem, datetime.timedelta):
                epoch = datetime.datetime.utcfromtimestamp(0)
                timedelta_from_epoch = epoch + elem
                rec[property_name] = timedelta_from_epoch.isoformat() + "+00:00"
            elif isinstance(elem, datetime.time):
                rec[property_name] = str(elem)
            elif isinstance(elem, bytes):
                # for BIT value, treat 0 as False and anything else as True
                bit_representation: bool
                if is_boolean_type(property_schema):
                    bit_representation = elem != b"\x00"
                    rec[property_name] = bit_representation
                else:
                    rec[property_name] = elem.hex()
            elif is_boolean_type(property_schema):
                boolean_representation: Optional[bool]
                if elem is None:
                    boolean_representation = None
                elif elem == 0:
                    boolean_representation = False
                else:
                    boolean_representation = True
                rec[property_name] = boolean_representation
            else:
                rec[property_name] = elem
        return rec

    # Public methods ("final", not recommended to be overridden)

    @final
    def sync(self):
        """Sync this stream."""
        self.logger.info(
            f"Beginning {self.replication_method} sync of stream '{self.name}'..."
        )
        # Send a SCHEMA message to the downstream target:
        self._write_schema_message()
        # Sync the records themselves:
        self._sync_records()

    # Overridable Methods

    def apply_catalog(self, catalog_dict: dict,) -> None:
        """Apply a catalog dict, updating any settings overridden within the catalog."""
        catalog = Catalog(catalog_dict)
        catalog_entry: singer.CatalogEntry = catalog.get_stream(self.name)
        self.primary_keys = catalog_entry.key_properties
        self.replication_key = catalog_entry.replication_key
        if catalog_entry.replication_method:
            self.forced_replication_method = catalog_entry.replication_method

    @property
    def records(self) -> Iterable[dict]:
        """Return a generator of row-type dictionary objects."""
        if self.partitions:
            for partition in self.partitions:
                partition_state = self.get_partition_state(partition)
                for row in self.get_records(partition_state):
                    row = self.post_process(row, partition_state)
                    yield row
        else:
            for row in self.get_records(self.stream_state):
                row = self.post_process(row, self.stream_state)
                yield row

    # Abstract Methods

    @abc.abstractmethod
    def get_records(self, partition: Optional[dict]) -> Iterable[Dict[str, Any]]:
        """Abstract row generator function. Must be overridden by the child class.

        Each row emitted should be a dictionary of property names to their values.
        """
        pass

    def post_process(self, row: dict, stream_or_partition_state: dict) -> dict:
        """Transform raw data from HTTP GET into the expected property values."""
        return row

    @property
    def http_headers(self) -> dict:
        """Return headers to be used by HTTP requests."""
        return NotImplemented()
