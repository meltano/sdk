"""TapStreamBase abstract class."""

import abc  # abstract base classes
import copy
import datetime
import json
import logging
import sys
from tap_base.helpers import classproperty
import time
from functools import lru_cache
from os import PathLike
from pathlib import Path
from typing import (
    Dict,
    Iterable,
    Any,
    List,
    Optional,
    Tuple,
    Callable,
    TypeVar,
    Union,
    cast,
)

import singer
from singer import CatalogEntry, RecordMessage, SchemaMessage
from singer import metadata
from singer.catalog import Catalog
from singer.schema import Schema

DEBUG_MODE = True

# How many records to emit between sending state message updates
STATE_MSG_FREQUENCY = 10 if DEBUG_MODE else 10000


FactoryType = TypeVar("FactoryType", bound="TapStreamBase")


class TapStreamBase(metaclass=abc.ABCMeta):
    """Abstract base class for tap streams."""

    MAX_CONNECT_RETRIES = 0

    tap_name: str = "sample-tap-name"  # For logging purposes
    discoverable: bool = False
    __logger: Optional[logging.Logger]

    # name: Optional[str] = None
    # schema_filepath: Optional[Union[str, PathLike]] = None
    # primary_keys: List[str] = []
    # replication_key: Optional[str] = None

    def __init__(
        self,
        config: dict,
        schema: Optional[Union[str, PathLike, Dict[str, Any], Schema]],
        name: Optional[str],
        state: Dict[str, Any],
    ):
        """Initialize tap stream."""
        if name:
            self.name: str = name
        if not self.name:
            raise ValueError("Missing argument or class variable 'name'.")
        self._config: dict = config
        self._state = state or {}
        self._schema: Optional[dict] = None
        self.forced_replication_method: Optional[str] = None
        self.replication_key: Optional[str] = None
        self.primary_keys: Optional[List[str]] = None
        self.__init_schema(schema)

    @classproperty
    def logger(self) -> logging.Logger:
        return logging.getLogger(self.tap_name)

    def __init_schema(
        self, schema: Union[str, PathLike, Dict[str, Any], Schema]
    ) -> None:
        if isinstance(schema, (str, PathLike)) and not self.schema_filepath:
            self.schema_filepath = schema
        if hasattr(self, "schema_filepath") and self.schema_filepath:
            self.schema_filepath = Path(self.schema_filepath)
            if not self.schema_filepath.exists():
                raise FileExistsError("Could not find schema file '{filepath}'.")
            self._schema = json.loads(self.schema_filepath.read_text())
        elif not schema:
            raise ValueError("Required parameter 'schema' not provided.")
        elif isinstance(schema, Schema):
            self._schema = schema.to_dict()
        elif isinstance(schema, dict):
            self._schema = schema
        else:
            raise ValueError(
                f"Unexpected type {type(schema).__name__} for arg 'schema'."
            )

    @property
    def schema(self) -> Optional[dict]:
        return self._schema

    def get_config(self, config_key: str, default: Any = None) -> Any:
        """Return config value or a default value."""
        return self._config.get(config_key, default)

    def get_stream_version(self):
        """Get stream version from bookmark."""
        stream_version = singer.get_bookmark(
            state=self._state, tap_stream_id=self.name, key="version"
        )
        if stream_version is None:
            stream_version = int(time.time() * 1000)
        return stream_version

    @property
    def tap_stream_id(self) -> str:
        return self.name

    @property
    def is_view(self) -> bool:
        return self.get_metadata("is-view", None)

    @property
    def replication_method(self) -> str:
        if self.forced_replication_method:
            return str(self.forced_replication_method)
        if self.replication_key:
            return "INCREMENTAL"
        return "FULL_TABLE"

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
    def singer_catalog_entry(self) -> CatalogEntry:
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

    def set_custom_metadata(self, md: Optional[dict]) -> None:
        if md:
            self._custom_metadata = md
        pk = self.get_metadata("key-properties", from_metadata_dict=md)
        replication_key = self.get_metadata("replication-key", from_metadata_dict=md)
        valid_bookmark_key = self.get_metadata(
            "valid-replication-keys", from_metadata_dict=md
        )
        method = self.get_metadata(
            "forced-replication-method", from_metadata_dict=md
        ) or self.get_metadata("replication-method", from_metadata_dict=md)
        if pk:
            self.primary_keys = pk
        if replication_key:
            self.replication_key = replication_key
        elif valid_bookmark_key:
            self.replication_key = valid_bookmark_key[0]
        if method:
            self.forced_replication_method = method

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
                singer.clear_bookmark(self._state, self.tap_stream_id, wipe_key)
            return
        elif except_keys:
            return self.wipe_bookmarks(
                [
                    found_key
                    for found_key in self._state.get("bookmarks", {})
                    .get(self.tap_stream_id, {})
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
            f"Beginning sync of '{self.name}' using "
            f"'{self.replication_method}' replication..."
        )
        if not initial_full_table_complete and not (
            version_exists and state_version is None
        ):
            singer.write_message(activate_version_message)
        self.write_schema_message()
        self.sync_records(self.get_record_generator, self.replication_method)
        singer.clear_bookmark(self._state, self.tap_stream_id, "max_pk_values")
        singer.clear_bookmark(self._state, self.tap_stream_id, "last_pk_fetched")
        singer.write_message(singer.StateMessage(value=copy.deepcopy(self._state)))
        singer.write_message(activate_version_message)

    def write_state_message(self):
        """Write out a STATE message with the latest state."""
        singer.write_message(singer.StateMessage(value=copy.deepcopy(self._state)))

    def write_schema_message(self):
        """Write out a SCHEMA message with the stream schema."""
        bookmark_keys = [self.replication_key] if self.replication_key else None
        schema_message = SchemaMessage(
            self.tap_stream_id, self.schema, self.primary_keys, bookmark_keys
        )
        singer.write_message(schema_message)

    def sync_records(self, record_generator: Callable, replication_method: str):
        """Sync records, emitting RECORD and STATE messages."""
        rows_sent = 0
        for row_dict in record_generator():
            row_dict = self.post_process(row_dict)
            if rows_sent and ((rows_sent - 1) % STATE_MSG_FREQUENCY == 0):
                self.write_state_message()
            record = self.conform_record_data_types(row_dict)
            record_message = RecordMessage(
                stream=self.name,
                record=record,
                version=None,
                time_extracted=datetime.datetime.now(datetime.timezone.utc),
            )
            # TODO: remove this temporary debug message
            # self.logger.info(row_dict)
            singer.write_message(record_message)
            rows_sent += 1
            self.update_state(record, replication_method)
        self.write_state_message()

    def update_state(
        self, record_message: singer.RecordMessage, replication_method: str
    ):
        """Update the stream's internal state with data from the provided record."""
        if not self._state:
            self._state = singer.write_bookmark(
                self._state, self.tap_stream_id, "version", self.get_stream_version(),
            )
        new_state = copy.deepcopy(self._state)
        if record_message:
            if replication_method == "FULL_TABLE":
                max_pk_values = singer.get_bookmark(
                    new_state, self.tap_stream_id, "max_pk_values"
                )
                if max_pk_values:
                    last_pk_fetched = {
                        k: v
                        for k, v in record_message.record.items()
                        if k in self.primary_keys
                    }
                    new_state = singer.write_bookmark(
                        new_state,
                        self.tap_stream_id,
                        "last_pk_fetched",
                        last_pk_fetched,
                    )
            elif replication_method == "INCREMENTAL":
                replication_key = self.replication_key
                if replication_key is not None:
                    new_state = singer.write_bookmark(
                        new_state,
                        self.tap_stream_id,
                        "replication_key",
                        replication_key,
                    )
                    new_state = singer.write_bookmark(
                        new_state,
                        self.tap_stream_id,
                        "replication_key_value",
                        record_message.record[replication_key],
                    )
        self._state = new_state
        return self._state

    def get_property_schema(self, property: str, warn=True) -> Optional[dict]:
        if property not in self.schema["properties"]:
            if warn:
                self.logger.debug(
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
    def conform_record_data_types(
        self,
        row: Dict[str, Any],
        # columns: List[str],
        version: int = None,
        time_extracted: datetime.datetime = datetime.datetime.now(
            datetime.timezone.utc
        ),
    ) -> RecordMessage:
        """Translate values in record dictionary to singer-compatible data types."""
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
        return rec

    # Class Factory Methods

    @classmethod
    def from_catalog_dict(
        cls, catalog_dict: dict, state: dict, logger: logging.Logger, config: dict
    ) -> Dict[str, FactoryType]:
        """Create a dictionary of stream objects from a catalog dictionary."""
        catalog = Catalog.from_dict(catalog_dict)
        result: Dict[str, FactoryType] = {}
        for catalog_entry in catalog.streams:
            stream_name = catalog_entry.stream_name
            result[stream_name] = cls.from_stream_dict(
                catalog_entry.to_dict(), state=state, config=config
            )
        return result

    @classmethod
    def from_stream_dict(
        cls, stream_dict: Dict[str, Any], config: dict, state: dict,
    ) -> FactoryType:
        """Create a stream object from a catalog's 'stream' dictionary entry."""
        stream_name = stream_dict.get("tap_stream_id", stream_dict.get("stream"))
        new_stream = cls(
            name=stream_name,
            schema=stream_dict.get("schema"),
            config=config,
            state=state,
        )
        new_stream.set_custom_metadata(stream_dict.get("metadata"))
        return cast(FactoryType, new_stream)

    # def is_connected(self) -> bool:
    #     """Return True if connected."""
    #     return self._conn is not None

    # def ensure_connected(self):
    #     """Connect if not yet connected."""
    #     if not self.is_connected():
    #         self.connect_with_retries()

    def fatal(self):
        """Fatal error. Abort stream."""
        sys.exit(1)

    # Abstract Methods

    @abc.abstractmethod
    def get_record_generator(self) -> Iterable[Iterable[Dict[str, Any]]]:
        """Abstract row generator function. Must be overridden by the child class.

        Each row emitted should be a dictionary of property names to their values.
        """
        pass

    def post_process(self, row: dict) -> dict:
        """Transform raw data from HTTP GET into the expected property values."""
        return row
