"""TapStreamBase abstract class"""

import abc  # abstract base classes
import time

import singer
from singer import CatalogEntry, StateMessage


class TapStreamBase(metaclass=abc.ABCMeta):
    """Abstract base class for tap streams."""

    _id: str
    _friendly_name: str
    _upstream_name: str
    _state: StateMessage
    _catalog_entry: CatalogEntry
    _latest_state: StateMessage

    def __init__(
        self,
        tap_stream_id: str,
        friendly_name: str = None,
        upstream_table_name: str = None,
        state: StateMessage = None,
    ):
        """Initialize tap stream."""
        self._catalog_entry = CatalogEntry(
            self,
            tap_stream_id=tap_stream_id,
            stream=friendly_name,
            stream_alias=None,
            key_properties=None,
            schema=None,
            replication_key=None,
            is_view=None,
            database=None,
            table=None,
            row_count=None,
            metadata=None,
            replication_method=None,
        )
        self._id = tap_stream_id
        self._state = state

    def get_catalog_entry_as_dict(self) -> dict:
        return self._catalog_entry.to_dict()

    def get_stream_version(self):
        """Get stream version from bookmark."""
        stream_version = singer.get_bookmark(
            state=self._state, tap_stream_id=self._id, key="version"
        )
        if stream_version is None:
            stream_version = int(time.time() * 1000)
        return stream_version

    def set_replication_method_and_key(stream, r_method, r_key):
        new_md = singer.metadata.to_map(stream.metadata)
        old_md = new_md.get(())
        if r_method:
            old_md.update({"replication-method": r_method})

        if r_key:
            old_md.update({"replication-key": r_key})

        stream.metadata = singer.metadata.to_list(new_md)
        return stream
