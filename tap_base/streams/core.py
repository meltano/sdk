"""Stream abstract class."""

import abc  # abstract base classes
import copy
import datetime
import json
import logging
from types import MappingProxyType
from tap_base.plugin_base import PluginBase as TapBaseClass
from tap_base.helpers import SecretString, get_property_schema, is_boolean_type
from tap_base import helpers
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
        self._config: dict = tap.config
        self._state = tap.state or {}
        self._schema: Optional[dict] = None
        self.forced_replication_method: Optional[str] = None
        self.replication_key: Optional[str] = None
        self.primary_keys: Optional[List[str]] = None
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
        if self.schema_filepath:
            if not Path(self.schema_filepath).is_file():
                raise FileExistsError(
                    f"Could not find schema file '{self.schema_filepath}'."
                )
            self._schema = json.loads(self.schema_filepath.read_text())
        elif not schema:
            raise ValueError(f"Required 'schema' not provided for '{self.name}'.")
        elif isinstance(schema, dict):
            self._schema = schema
        elif isinstance(schema, Schema):
            self._schema = schema.to_dict()
        else:
            raise ValueError(
                f"Unexpected type {type(schema).__name__} for arg 'schema'."
            )

    @property
    def schema(self) -> Optional[dict]:
        """Return the schema dict for the stream."""
        return self._schema

    @property
    @lru_cache()
    def config(self) -> Mapping[str, Any]:
        """Return a frozen (read-only) config dictionary map."""
        return MappingProxyType(self._config)

    def get_query_params(self, substream_id: Optional[str]) -> dict:
        """By default, return all config values which are not secrets."""
        return {k: v for k, v in self.config.items() if not isinstance(v, SecretString)}

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

    # Substreams

    def get_substream_ids(self) -> Optional[List[str]]:
        """Return a list of substream IDs (if applicable), otherwise None."""
        return helpers.get_state_substream_ids(self._state, self.name)

    # Private methods

    def _wipe_bookmarks(
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
            return self._wipe_bookmarks(
                [
                    found_key
                    for found_key in self._state.get("bookmarks", {})
                    .get(self.tap_stream_id, {})
                    .keys()
                    if found_key not in except_keys
                ]
            )

    def _get_bookmark(self, key: str, default: Any = None):
        """Return a bookmark key's value, or a default value if key is not set."""
        return singer.get_bookmark(
            state=self._state,
            tap_stream_id=self.tap_stream_id,
            key=key,
            default=default,
        )

    def _write_state_message(self):
        """Write out a STATE message with the latest state."""
        singer.write_message(singer.StateMessage(value=copy.deepcopy(self._state)))

    def _write_schema_message(self):
        """Write out a SCHEMA message with the stream schema."""
        bookmark_keys = [self.replication_key] if self.replication_key else None
        schema_message = SchemaMessage(
            self.tap_stream_id, self.schema, self.primary_keys, bookmark_keys
        )
        singer.write_message(schema_message)

    def _sync_records(self):
        """Sync records, emitting RECORD and STATE messages."""
        rows_sent = 0
        for row_dict in self.records:
            row_dict = self.post_process(row_dict)
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
            rows_sent += 1
            # TODO: Fix bookmark state updates
            # self.tap._update_state(record, self.replication_method)
        self._write_state_message()

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
        self._wipe_bookmarks(
            except_keys=[
                "last_pk_fetched",
                "max_pk_values",
                "version",
                "initial_full_table_complete",
            ]
        )
        bookmark = self._state.get("bookmarks", {}).get(self.tap_stream_id, {})
        version_exists = True if "version" in bookmark else False
        initial_full_table_complete = self._get_bookmark("initial_full_table_complete")
        state_version = self._get_bookmark("version")
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
        self._write_schema_message()
        self._sync_records()
        singer.clear_bookmark(self._state, self.tap_stream_id, "max_pk_values")
        singer.clear_bookmark(self._state, self.tap_stream_id, "last_pk_fetched")
        self._write_state_message()
        singer.write_message(activate_version_message)

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

    def post_process(self, row: dict) -> dict:
        """Transform raw data from HTTP GET into the expected property values."""
        return row

    @property
    def http_headers(self) -> dict:
        """Return headers to be used by HTTP requests."""
        return NotImplemented()

    # Deprecated (TODO: Merge `set_custom_metadata()` with `apply_catalog()`)

    # def set_custom_metadata(self, md: Optional[dict]) -> None:
    #     if md:
    #         self._custom_metadata = md
    #     pk = self.get_metadata("key-properties", from_metadata_dict=md)
    #     replication_key = self.get_metadata("replication-key", from_metadata_dict=md)
    #     valid_bookmark_key = self.get_metadata(
    #         "valid-replication-keys", from_metadata_dict=md
    #     )
    #     method = self.get_metadata(
    #         "forced-replication-method", from_metadata_dict=md
    #     ) or self.get_metadata("replication-method", from_metadata_dict=md)
    #     if pk:
    #         self.primary_keys = pk
    #     if replication_key:
    #         self.replication_key = replication_key
    #     elif valid_bookmark_key:
    #         self.replication_key = valid_bookmark_key[0]
    #     if method:
    #         self.forced_replication_method = method

    # Deprecated (TODO: DELETE)

    # def _get_metadata(
    #     self,
    #     key_name,
    #     default: Any = None,
    #     breadcrumb: Tuple = (),
    #     from_metadata_dict: dict = None,
    # ):
    #     """Return top level metadata (breadcrumb="()")."""
    #     if not from_metadata_dict:
    #         md_dict = self._custom_metadata
    #     else:
    #         md_dict = from_metadata_dict
    #     if not md_dict:
    #         return default
    #     md_map = metadata.to_map(md_dict)
    #     if not md_map:
    #         self.logger.warning(f"Could not find '{key_name}' metadata.")
    #         return default
    #     result = md_map.get(breadcrumb, {}).get(key_name, default)
    #     return result
