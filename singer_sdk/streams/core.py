"""Stream abstract class."""

import abc  # abstract base classes
import datetime
import json
import logging
from types import MappingProxyType
from singer_sdk.plugin_base import PluginBase as TapBaseClass
from singer_sdk.helpers import SecretString, get_property_schema, is_boolean_type
from singer_sdk import helpers
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
except:
    # Final not available until Python3.8
    final = lambda x: x

import singer
from singer import CatalogEntry, RecordMessage, SchemaMessage
from singer import metadata
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
        self._schema: Optional[dict] = None
        self.forced_replication_method: Optional[str] = None
        self.replication_key: Optional[str] = None
        self.primary_keys: Optional[List[str]] = None
        self.active_partition: Optional[dict] = None
        if not hasattr(self, "schema_filepath"):  # Skip if set at the class level.
            self.schema_filepath: Optional[Path] = None
        self.__init_schema(schema)

    def __init_schema(
        self, schema: Union[str, PathLike, Dict[str, Any], Schema]
    ) -> None:
        if isinstance(schema, (str, PathLike)) and not self.schema_filepath:
            self.schema_filepath = Path(schema)
        if isinstance(self.schema_filepath, str):
            self.schema_filepath = Path(self.schema_filepath)
        self._schema = None
        if self.schema_filepath:
            if not Path(self.schema_filepath).is_file():
                raise FileExistsError(
                    f"Could not find schema file '{self.schema_filepath}'."
                )
            self._schema = json.loads(self.schema_filepath.read_text())
        elif isinstance(schema, dict):
            self._schema = schema
        elif isinstance(schema, Schema):
            self._schema = schema.to_dict()
        elif schema:
            raise ValueError(
                f"Unexpected type {type(schema).__name__} for arg 'schema'."
            )

    @property
    def schema(self) -> dict:
        """Return the schema dict for the stream."""
        if not self._schema:
            raise ValueError(f"Required 'schema' not provided for '{self.name}'.")
        return self._schema

    @property
    def config(self) -> Mapping[str, Any]:
        """Return a frozen (read-only) config dictionary map."""
        return MappingProxyType(self._config)

    def get_params(self, stream_or_partition_state: dict) -> dict:
        """Return a dictionary of values to be used in parameterization.

        By default, this includes all settings which are secrets and any stored values
        the stream or partition state, as passed via the `stream_or_partition_state`
        argument.
        """
        result = {
            k: v for k, v in self.config.items() if not isinstance(v, SecretString)
        }
        result.update(stream_or_partition_state)
        return result

    def get_stream_version(self):
        """Get stream version from bookmark."""
        stream_version = self._get_bookmark("version")
        if stream_version is None:
            stream_version = int(time.time() * 1000)
        return stream_version

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

    # Singer spec properties

    @final
    @property
    def singer_metadata(self) -> dict:
        """Return the metadata dict from the singer spec."""
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

    @final
    @property
    def singer_catalog_entry(self) -> CatalogEntry:
        """Return a singer CatalogEntry object."""
        return CatalogEntry(
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

    # State properties:

    @property
    def tap_state(self) -> dict:
        """Return a writeable state dict for the entire tap.

        Note: This dictionary is shared (and writable) across all streams.
        """
        return self._tap_state

    @property
    def stream_state(self) -> dict:
        """Return a writeable state dict for this stream.

        A blank state entry will be created if one doesn't already exist.
        """
        return helpers.get_stream_state_dict(self.tap_state, self.name)

    def get_partition_state(self, partition_keys: dict) -> dict:
        """Return a writable state dict for the given partition."""
        return helpers.get_stream_state_dict(
            self.tap_state, self.name, partition_keys=partition_keys
        )

    # Partitions

    def get_partitions_list(self) -> Optional[List[dict]]:
        """Return a list of partition key dicts (if applicable), otherwise None."""
        state = helpers.read_stream_state(self.tap_state, self.name)
        if "partitions" not in state:
            return None
        result: List[dict] = []
        for partition_state in state["partitions"]:
            result.append(partition_state.get("context"))
        return result

    # Private bookmarking methods

    def _increment_stream_state(
        self, latest_record: Dict[str, Any], *, partition_keys: Optional[dict] = None
    ):
        """Update state of the stream or partition with data from the provided record."""
        if partition_keys:
            state_dict = self.get_partition_state(partition_keys)
        else:
            state_dict = self.stream_state
        if latest_record:
            if self.replication_method == "FULL_TABLE":
                max_pk_values = singer._get_bookmark("max_pk_values")
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

    def _sync_records(self):
        """Sync records, emitting RECORD and STATE messages."""
        rows_sent = 0
        # Reset interim state keys from prior executions:
        helpers.wipe_stream_state_keys(
            self.tap_state,
            self.name,
            except_keys=[
                "last_pk_fetched",
                "max_pk_values",
                "version",
                "initial_full_table_complete",
            ],
            partition_keys=self.active_partition,
        )
        # Iterate through each returned record:
        for row_dict in self.records:
            if rows_sent and ((rows_sent - 1) % STATE_MSG_FREQUENCY == 0):
                self._write_state_message()
            record = self._conform_record_data_types(row_dict)
            record_message = RecordMessage(
                stream=self.name,
                record=record,
                version=None,
                time_extracted=datetime.datetime.now(datetime.timezone.utc),
            )
            singer.write_message(record_message)
            self._increment_stream_state(
                record, self.replication_method, partition_keys=self.active_partition
            )
            rows_sent += 1
        self.logger.info(f"Completed '{self.name}' sync ({rows_sent} records).")
        # Reset interim bookmarks before emitting final STATE message:
        helpers.wipe_stream_state_keys(
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

    # Abstract Methods

    @abc.abstractproperty
    def records(self) -> Iterable[Iterable[Dict[str, Any]]]:
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
