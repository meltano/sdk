"""TapStreamBase abstract class."""

import abc  # abstract base classes
import copy
import datetime
from functools import lru_cache
import logging

# from functools import lru_cache
import time
from typing import Dict, Iterable, Any, List, Optional, Tuple, Callable, TypeVar, Union

import singer
from singer import CatalogEntry, RecordMessage, SchemaMessage
from singer import metadata
from singer.catalog import Catalog
from singer.schema import Schema

from tap_base.stream_base import GenericStreamBase

DEBUG_MODE = True

# How many records to emit between sending state message updates
STATE_MSG_FREQUENCY = 10 if DEBUG_MODE else 10000


FactoryType = TypeVar("FactoryType", bound="TapStreamBase")


class TapStreamBase(GenericStreamBase, metaclass=abc.ABCMeta):
    """Abstract base class for tap streams."""

    _tap_stream_id: str
    _state: dict
    _schema: dict
    _key_properties: List[str] = []
    _replication_key: Optional[str] = None
    _custom_metadata: Optional[dict] = None

    logger: logging.Logger

    def __init__(
        self,
        tap_stream_id: str,
        schema: Union[Dict[str, Any], Schema],
        state: Dict[str, Any],
        logger: logging.Logger,
        config: dict,
    ):
        """Initialize tap stream."""
        super().__init__(
            config=config, logger=logger,
        )
        self._tap_stream_id = tap_stream_id
        self._schema = schema.to_dict() if isinstance(schema, Schema) else schema
        self._state = state or {}

    def get_stream_version(self):
        """Get stream version from bookmark."""
        stream_version = singer.get_bookmark(
            state=self._state, tap_stream_id=self._tap_stream_id, key="version"
        )
        if stream_version is None:
            stream_version = int(time.time() * 1000)
        return stream_version

    @property
    def tap_stream_id(self) -> str:
        return self._tap_stream_id

    @property
    def stream_name(self) -> str:
        return self._tap_stream_id

    @property
    def is_view(self) -> bool:
        return self.get_metadata("is-view", None)

    @property
    def primary_key(self) -> List[str]:
        metadata_val = self.get_metadata(
            "view-key-properties" if self.is_view else "table-key-properties", ()
        )
        return self._key_properties or metadata_val

    @primary_key.setter
    def primary_key(self, pk: List[str]):
        self._key_properties = pk

    @property
    def bookmark_key(self):
        state_key = singer.get_bookmark(
            self._state, self.tap_stream_id, "replication_key"
        )
        metadata_key = self.get_metadata("replication-key")
        return self._replication_key or state_key or metadata_key

    @bookmark_key.setter
    def bookmark_key(self, key_name: str):
        self._replication_key = key_name

    @property
    def schema(self) -> dict:
        return self._schema

    @property
    def replication_method(self) -> str:
        if self.forced_replication_method:
            return str(self.forced_replication_method)
        if self.bookmark_key:
            return "INCREMENTAL"
        return "FULL_TABLE"

    @property
    def forced_replication_method(self) -> Optional[str]:
        return self.get_metadata("forced-replication-method")

    @property
    def singer_metadata(self) -> dict:
        md = metadata.get_standard_metadata(
            schema=self.schema,
            replication_method=self.replication_method,
            key_properties=self.primary_key or None,
            valid_replication_keys=[self.bookmark_key] if self.bookmark_key else None,
            schema_name=None,
        )
        return md

    @property
    def singer_catalog_entry(self) -> CatalogEntry:
        return CatalogEntry(
            tap_stream_id=self._tap_stream_id,
            stream=self._tap_stream_id,
            schema=Schema.from_dict(self.schema),
            metadata=self.singer_metadata,
            key_properties=self.primary_key or None,
            replication_key=self.bookmark_key,
            replication_method=self.replication_method,
            is_view=None,
            database=None,
            table=None,
            row_count=None,
            stream_alias=None,
        )

    def set_custom_metadata(self, md: Optional[dict]) -> None:
        if md:
            self._custom_metadata = md
        pk = self.get_metadata("key-properties", from_metadata_dict=md)
        bookmark_key = self.get_metadata("replication-key", from_metadata_dict=md)
        valid_bookmark_key = self.get_metadata(
            "valid-replication-keys", from_metadata_dict=md
        )
        method = self.get_metadata("forced-replication-method", from_metadata_dict=md)
        method = method or self.get_metadata(
            "replication-method", from_metadata_dict=md
        )
        if pk:
            self.primary_key = pk
        if bookmark_key:
            self.bookmark_key = bookmark_key
        elif valid_bookmark_key:
            self.bookmark_key = valid_bookmark_key[0]

    # @lru_cache
    def get_metadata(
        self,
        key_name,
        default: Any = None,
        breadcrumb: Tuple = (),
        from_metadata_dict: dict = None,
    ):
        """Return top level metadata (breadcrumb="()")."""
        if not from_metadata_dict:
            md_dict = self._custom_metadata
        else:
            md_dict = from_metadata_dict
        if not md_dict:
            return default
        md_map = metadata.to_map(md_dict)
        if not md_map:
            self.logger.warning(f"Could not find '{key_name}' metadata.")
            return default
        result = md_map.get(breadcrumb, {}).get(key_name, default)
        return result

    def wipe_bookmarks(
        self, wipe_keys: List[str] = None, *, except_keys: List[str] = None,
    ) -> None:
        """Wipe bookmarks.

        You may specify a list to wipe or a list to keep, but not both.
        """

        def _bad_args():
            raise ValueError(
                "Incorrect number of arguments. "
                "Expected `except_keys` or `wipe_keys` but not both."
            )

        if except_keys and wipe_keys:
            _bad_args()
        elif wipe_keys:
            for wipe_key in wipe_keys:
                singer.clear_bookmark(self._state, self._tap_stream_id, wipe_key)
            return
        elif except_keys:
            return self.wipe_bookmarks(
                [
                    found_key
                    for found_key in self._state.get("bookmarks", {})
                    .get(self._tap_stream_id, {})
                    .keys()
                    if found_key not in except_keys
                ]
            )

    def sync(self):
        """Sync this stream."""
        self.wipe_bookmarks(
            except_keys=[
                "last_pk_fetched",
                "max_pk_values",
                "version",
                "initial_full_table_complete",
            ]
        )
        bookmark = self._state.get("bookmarks", {}).get(self.tap_stream_id, {})
        version_exists = True if "version" in bookmark else False
        initial_full_table_complete = singer.get_bookmark(
            self._state, self.tap_stream_id, "initial_full_table_complete",
        )
        state_version = singer.get_bookmark(self._state, self.tap_stream_id, "version")
        activate_version_message = singer.ActivateVersionMessage(
            stream=self.tap_stream_id, version=self.get_stream_version()
        )
        self.logger.info(
            f"Beginning sync of '{self.tap_stream_id}' using "
            f"'{self.replication_method}' replication..."
        )
        if not initial_full_table_complete and not (
            version_exists and state_version is None
        ):
            singer.write_message(activate_version_message)
        self.write_schema_message()
        self.sync_records(self.get_row_generator, self.replication_method)
        singer.clear_bookmark(self._state, self._tap_stream_id, "max_pk_values")
        singer.clear_bookmark(self._state, self._tap_stream_id, "last_pk_fetched")
        singer.write_message(singer.StateMessage(value=copy.deepcopy(self._state)))
        singer.write_message(activate_version_message)

    def write_state_message(self):
        """Write out a STATE message with the latest state."""
        singer.write_message(singer.StateMessage(value=copy.deepcopy(self._state)))

    def write_schema_message(self):
        """Write out a SCHEMA message with the stream schema."""
        bookmark_keys = [self.bookmark_key] if self.bookmark_key else None
        schema_message = SchemaMessage(
            self.tap_stream_id, self.schema, self.primary_key, bookmark_keys
        )
        singer.write_message(schema_message)

    def sync_records(self, row_generator: Callable, replication_method: str):
        """Sync records, emitting RECORD and STATE messages."""
        rows_sent = 0
        for row_dict in row_generator():
            if rows_sent and ((rows_sent - 1) % STATE_MSG_FREQUENCY == 0):
                self.write_state_message()
            record = self.transform_row_to_singer_record(row=row_dict)
            singer.write_message(record)
            rows_sent += 1
            self.update_state(record, replication_method)
        self.write_state_message()

    def update_state(
        self, record_message: singer.RecordMessage, replication_method: str
    ):
        """Update the stream's internal state with data from the provided record."""
        if not self._state:
            self._state = singer.write_bookmark(
                self._state, self._tap_stream_id, "version", self.get_stream_version(),
            )
        new_state = copy.deepcopy(self._state)
        if record_message:
            if replication_method == "FULL_TABLE":
                max_pk_values = singer.get_bookmark(
                    new_state, self._tap_stream_id, "max_pk_values"
                )
                if max_pk_values:
                    last_pk_fetched = {
                        k: v
                        for k, v in record_message.record.items()
                        if k in self.primary_key
                    }
                    new_state = singer.write_bookmark(
                        new_state,
                        self._tap_stream_id,
                        "last_pk_fetched",
                        last_pk_fetched,
                    )
            elif replication_method == "INCREMENTAL":
                replication_key = self.bookmark_key
                if replication_key is not None:
                    new_state = singer.write_bookmark(
                        new_state,
                        self._tap_stream_id,
                        "replication_key",
                        replication_key,
                    )
                    new_state = singer.write_bookmark(
                        new_state,
                        self._tap_stream_id,
                        "replication_key_value",
                        record_message.record[replication_key],
                    )
        self._state = new_state
        return self._state

    @lru_cache
    def get_property_schema(self, property: str, warn=True) -> Optional[dict]:
        if property not in self.schema["properties"]:
            if warn:
                self.logger.warning(
                    f"Could not locate schema mapping for property '{property}'. "
                    "Any corresponding data will be excluded from the stream..."
                )
            return None
        return self.schema["properties"][property]

    def is_boolean_type(self, property_schema: dict) -> bool:
        if "anyOf" not in property_schema and "type" not in property_schema:
            logging.warning(
                f"Could not detect data type in property schema: {property_schema}"
            )
            return False
        for property_type in property_schema.get(
            "anyOf", [property_schema.get("type")]
        ):
            if "boolean" in property_type or property_type == "boolean":
                return True
        return False

    # pylint: disable=too-many-branches
    def transform_row_to_singer_record(
        self,
        row: Dict[str, Any],
        # columns: List[str],
        version: int = None,
        time_extracted: datetime.datetime = datetime.datetime.now(
            datetime.timezone.utc
        ),
    ) -> RecordMessage:
        """Transform dictionary row data into a singer-compatible RECORD message."""
        rec: Dict[str, Any] = {}
        for property_name, elem in row.items():
            property_schema = self.get_property_schema(property_name)
            if not property_schema:
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
                if self.is_boolean_type(property_schema):
                    bit_representation = elem != b"\x00"
                    rec[property_name] = bit_representation
                else:
                    rec[property_name] = elem.hex()
            elif self.is_boolean_type(property_schema):
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

        # rec = dict(zip(columns, row_to_persist))
        return RecordMessage(
            stream=self.stream_name,
            record=rec,
            version=version,
            time_extracted=time_extracted,
        )

    # Class Factory Methods

    @classmethod
    def from_catalog_dict(
        cls, catalog_dict: Dict[str, Any], state: dict, logger: logging.Logger,
    ) -> Dict[str, FactoryType]:
        """Create a dictionary of stream objects from a catalog dictionary."""
        catalog = Catalog.from_dict(catalog_dict)
        result: Dict[str, FactoryType] = {}
        for catalog_entry in catalog.streams:
            stream_name = catalog_entry.stream_name
            result[stream_name] = cls.from_stream_dict(
                catalog_entry.to_dict(), state=state, logger=logger
            )
        return result

    @classmethod
    def from_stream_dict(
        cls,
        stream_dict: Dict[str, Any],
        config: dict,
        state: dict,
        logger: logging.Logger,
    ) -> FactoryType:
        """Create a stream object from a catalog's 'stream' dictionary entry."""
        stream_name = stream_dict.get("tap_stream_id", stream_dict.get("stream"))
        new_stream = cls(
            tap_stream_id=stream_name,
            schema=stream_dict.get("schema"),
            config=config,
            state=state,
            logger=logger,
        )
        new_stream.set_custom_metadata(stream_dict.get("metadata"))
        return new_stream

    # Abstract Methods

    @abc.abstractmethod
    def get_row_generator(self) -> Iterable[Iterable[Dict[str, Any]]]:
        """Abstract row generator function. Must be overridden by the child class.

        Each row emitted should be a dictionary of property names to their values.
        """
        pass
