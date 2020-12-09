"""TapStreamBase abstract class."""

import abc  # abstract base classes
import copy
import datetime
import logging

# from functools import lru_cache
import time
from typing import Dict, Iterable, Any, List, Optional, Tuple, Callable

import singer
from singer import CatalogEntry, RecordMessage
from singer import metadata

from tap_base.connection_base import GenericConnectionBase

DEBUG_MODE = True

# How many records to emit between sending state message updates
STATE_MSG_FREQUENCY = 10 if DEBUG_MODE else 10000


class TapStreamBase(metaclass=abc.ABCMeta):
    """Abstract base class for tap streams."""

    _tap_stream_id: str
    _catalog_entry: CatalogEntry
    _state: dict

    logger: logging.Logger

    def __init__(
        self,
        tap_stream_id: str,
        connection: GenericConnectionBase,
        catalog_entry: CatalogEntry,
        state: dict,
        logger: logging.Logger,
    ):
        """Initialize tap stream."""
        self._tap_stream_id = tap_stream_id
        self._conn = connection
        self._catalog_entry = catalog_entry
        self._state = state or {}
        self.logger = logger

    def get_stream_version(self):
        """Get stream version from bookmark."""
        stream_version = singer.get_bookmark(
            state=self._state, tap_stream_id=self._tap_stream_id, key="version"
        )
        if stream_version is None:
            stream_version = int(time.time() * 1000)
        return stream_version

    # @lru_cache
    def get_metadata(self, key_name, default: Any = None, breadcrumb: Tuple = ()):
        """Return top level metadata (breadcrumb="()")."""
        md_map = metadata.to_map(self._catalog_entry.metadata)
        if not md_map:
            self.logger.warning(f"Could not find '{key_name}' metadata.")
            return default
        result = md_map.get(breadcrumb, {}).get(key_name, default)
        # self.logger.info(
        #     f"DEBUG: Search for '{breadcrumb}'->'{key_name}' "
        #     f"resulted in {result}. md_map was {md_map}"
        # )
        return result

    def get_replication_method(self) -> str:
        """Return the stream's replication method."""
        return (
            self.get_metadata("forced-replication-method")
            or self.get_metadata("replication-method")
            or self._catalog_entry.replication_method
        )

    def get_replication_key(self) -> str:
        """Return the stream's replication key."""
        state_key = singer.get_bookmark(
            self._state, self._catalog_entry.tap_stream_id, "replication_key"
        )
        stream_key = self._catalog_entry.replication_key
        metadata_key = self.get_metadata("replication-key")
        return state_key or stream_key or metadata_key

    # @lru_cache
    def get_key_properties(self) -> List[str]:
        """Get key properties from catalog."""
        is_view = self.get_metadata("is-view", None)
        metadata_val = self.get_metadata(
            "view-key-properties" if is_view else "table-key-properties", ()
        )
        catalog_val = self._catalog_entry.key_properties
        return catalog_val or metadata_val

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
        bookmark = self._state.get("bookmarks", {}).get(
            self._catalog_entry.tap_stream_id, {}
        )
        version_exists = True if "version" in bookmark else False
        initial_full_table_complete = singer.get_bookmark(
            self._state,
            self._catalog_entry.tap_stream_id,
            "initial_full_table_complete",
        )
        state_version = singer.get_bookmark(
            self._state, self._catalog_entry.tap_stream_id, "version"
        )
        activate_version_message = singer.ActivateVersionMessage(
            stream=self._catalog_entry.stream, version=self.get_stream_version()
        )
        replication_method = self.get_replication_method()
        self.logger.info(
            f"Beginning sync of '{self._tap_stream_id}' using "
            f"'{replication_method}' replication..."
        )
        if not initial_full_table_complete and not (
            version_exists and state_version is None
        ):
            singer.write_message(activate_version_message)
        self.sync_records(self.get_row_generator, replication_method)
        singer.clear_bookmark(self._state, self._tap_stream_id, "max_pk_values")
        singer.clear_bookmark(self._state, self._tap_stream_id, "last_pk_fetched")
        singer.write_message(singer.StateMessage(value=copy.deepcopy(self._state)))
        singer.write_message(activate_version_message)

    def write_state_message(self):
        """Write out a state message with the latest state."""
        singer.write_message(singer.StateMessage(value=copy.deepcopy(self._state)))

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
                key_properties = self.get_key_properties()
                max_pk_values = singer.get_bookmark(
                    new_state, self._tap_stream_id, "max_pk_values"
                )
                if max_pk_values:
                    last_pk_fetched = {
                        k: v
                        for k, v in record_message.record.items()
                        if k in key_properties
                    }
                    new_state = singer.write_bookmark(
                        new_state,
                        self._tap_stream_id,
                        "last_pk_fetched",
                        last_pk_fetched,
                    )
            elif replication_method == "INCREMENTAL":
                replication_key = self.get_replication_key()
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
            property_type = self._catalog_entry.schema.properties[property_name].type
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
                if "boolean" in property_type:
                    bit_representation = elem != b"\x00"
                    rec[property_name] = bit_representation
                else:
                    rec[property_name] = elem.hex()
            elif "boolean" in property_type or property_type == "boolean":
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
            stream=self._catalog_entry.stream,
            record=rec,
            version=version,
            time_extracted=time_extracted,
        )

    ###########################
    #### Abstract Methods #####
    ###########################

    @abc.abstractmethod
    def get_row_generator(self) -> Iterable[Iterable[Dict[str, Any]]]:
        """Abstract row generator function. Must be overridden by the child class.

        Each row emitted should be a dictionary of property names to their values.
        """
        pass
