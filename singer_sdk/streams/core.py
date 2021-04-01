"""Stream abstract class."""

import abc  # abstract base classes
import datetime
import json
import logging
from os import PathLike
from pathlib import Path
from types import MappingProxyType
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

import pendulum
from singer import metadata

from singer_sdk.helpers._typing import conform_record_data_types
from singer_sdk.helpers._state import (
    get_writeable_state_dict,
    wipe_stream_state_keys,
    get_state_partitions_list,
)
from singer_sdk.plugin_base import PluginBase as TapBaseClass
from singer_sdk.helpers._compat import final

import singer
from singer import RecordMessage, SchemaMessage
from singer.catalog import Catalog
from singer.schema import Schema
from singer_sdk.helpers._typing import is_datetime_type


# Replication methods
REPLICATION_FULL_TABLE = "FULL_TABLE"
REPLICATION_INCREMENTAL = "INCREMENTAL"
REPLICATION_LOG_BASED = "LOG_BASED"

FactoryType = TypeVar("FactoryType", bound="Stream")


class Stream(metaclass=abc.ABCMeta):
    """Abstract base class for tap streams."""

    STATE_MSG_FREQUENCY = 10000  # Number of records between state messages
    MAX_RECORDS_LIMIT: Optional[int] = None

    parent_stream_types: List[Any] = []  # May be used in sync sequencing

    def __init__(
        self,
        tap: TapBaseClass,
        schema: Optional[Union[str, PathLike, Dict[str, Any], Schema]] = None,
        name: Optional[str] = None,
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
        self._replication_key: Optional[str] = None
        self._primary_keys: Optional[List[str]] = None
        self._schema_filepath: Optional[Path] = None
        self._schema: Optional[dict] = None
        if schema:
            if isinstance(schema, (PathLike, str)):
                self._schema_filepath = Path(schema)
            elif isinstance(schema, dict):
                self._schema = schema
            elif isinstance(schema, Schema):
                self._schema = schema.to_dict()
            else:
                raise ValueError(
                    f"Unexpected type {type(schema).__name__} for arg 'schema'."
                )

    @property
    def is_timestamp_replication_key(self) -> bool:
        """Return True if the stream uses a timestamp-based replication key.

        Developers can override with `is_timestamp_replication_key = True` in
        order to force this value.
        """
        if not self.replication_key:
            return False
        type_dict = self.schema.get("properties", {}).get(self.replication_key)
        return is_datetime_type(type_dict)

    def get_starting_timestamp(
        self, partition: Optional[dict]
    ) -> Optional[datetime.datetime]:
        """Return `start_date` config, or state if using timestamp replication."""
        if self.is_timestamp_replication_key:
            state = self.get_stream_or_partition_state(partition)
            replication_key = state.get("replication_key")
            if replication_key and replication_key in state:
                return pendulum.parse(state[replication_key])

        if "start_date" in self.config:
            return pendulum.parse(self.config["start_date"])

        return None

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
    def primary_keys(self) -> Optional[List[str]]:
        """Return primary key(s) for the stream."""
        if not self._primary_keys:
            return None
        return self._primary_keys

    @primary_keys.setter
    def primary_keys(self, new_value: List[str]):
        """Set primary key(s) for the stream."""
        self._primary_keys = new_value

    @property
    def replication_key(self) -> Optional[str]:
        """Return replication key for the stream."""
        if not self._replication_key:
            return None
        return self._replication_key

    @replication_key.setter
    def replication_key(self, new_value: str) -> None:
        """Set replication key for the stream."""
        self._replication_key = new_value

    @property
    def _singer_metadata(self) -> dict:
        """Return metadata object (dict) as specified in the Singer spec."""
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
    def _singer_catalog_entry(self) -> singer.CatalogEntry:
        """Return catalog entry as specified by the Singer catalog spec."""
        return singer.CatalogEntry(
            tap_stream_id=self.tap_stream_id,
            stream=self.name,
            schema=Schema.from_dict(self.schema),
            metadata=self._singer_metadata,
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
            return REPLICATION_INCREMENTAL
        return REPLICATION_FULL_TABLE

    # State properties:

    @property
    def tap_state(self) -> dict:
        """Return a writeable state dict for the entire tap.

        Note: This dictionary is shared (and writable) across all streams.
        """
        return self._tap_state

    def get_stream_or_partition_state(self, partition: Optional[dict]) -> dict:
        """Return partition state if applicable; else return stream state."""
        if partition:
            return self.get_partition_state(partition)
        return self.stream_state

    @property
    def stream_state(self) -> dict:
        """Return a writeable state dict for this stream.

        A blank state entry will be created if one doesn't already exist.
        """
        return get_writeable_state_dict(self.tap_state, self.name)

    def get_partition_state(self, partition: dict) -> dict:
        """Return a writable state dict for the given partition."""
        return get_writeable_state_dict(self.tap_state, self.name, partition=partition)

    # Partitions

    @property
    def partitions(self) -> Optional[List[dict]]:
        """Return a list of partition key dicts (if applicable), otherwise None.

        By default, this method returns a list of any partitions which are already
        defined in state, otherwise None.
        Developers may override this property to provide a default partitions list.
        """
        result: List[dict] = []
        for partition_state in (
            get_state_partitions_list(self.tap_state, self.name) or []
        ):
            result.append(partition_state["context"])
        return result or None

    # Private bookmarking methods

    def _increment_stream_state(
        self, latest_record: Dict[str, Any], *, partition: Optional[dict] = None
    ):
        """Update state of stream or partition with data from the provided record."""
        state_dict = self.get_stream_or_partition_state(partition)
        if latest_record:
            if self.replication_method in [
                REPLICATION_INCREMENTAL,
                REPLICATION_LOG_BASED,
            ]:
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
            partition=partition,
            wipe_keys=[
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
                if (
                    self.MAX_RECORDS_LIMIT is not None
                    and rows_sent >= self.MAX_RECORDS_LIMIT
                ):
                    logging.info(
                        "Stream prematurely aborted due to the stream's max record "
                        f"limit ({self.MAX_RECORDS_LIMIT}) being reached."
                    )
                    # Abort stream sync for this partition or stream
                    break

                if rows_sent and ((rows_sent - 1) % self.STATE_MSG_FREQUENCY == 0):
                    self._write_state_message()
                record = conform_record_data_types(
                    stream_name=self.name,
                    row=row_dict,
                    schema=self.schema,
                    logger=self.logger,
                )
                record_message = RecordMessage(
                    stream=self.name,
                    record=record,
                    version=None,
                    time_extracted=pendulum.now(),
                )
                singer.write_message(record_message)
                self._increment_stream_state(record, partition=partition)
                rows_sent += 1
            wipe_stream_state_keys(
                self.tap_state,
                self.name,
                partition=partition,
                wipe_keys=["last_pk_fetched", "max_pk_values"],
            )
        self.logger.info(f"Completed '{self.name}' sync ({rows_sent} records).")
        # Reset interim bookmarks before emitting final STATE message:
        self._write_state_message()

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

    def apply_catalog(self, catalog_dict: dict) -> None:
        """Apply a catalog dict, updating any settings overridden within the catalog."""
        catalog = Catalog.from_dict(catalog_dict)
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
                for row in self.get_records(partition):
                    row = self.post_process(row, partition)
                    yield row
        else:
            for row in self.get_records():
                row = self.post_process(row)
                yield row

    # Abstract Methods

    @abc.abstractmethod
    def get_records(self, partition: Optional[dict] = None) -> Iterable[Dict[str, Any]]:
        """Abstract row generator function. Must be overridden by the child class.

        Each row emitted should be a dictionary of property names to their values.
        """
        pass

    def post_process(self, row: dict, partition: Optional[dict] = None) -> dict:
        """As needed, append or transform raw data to match expected structure."""
        return row
