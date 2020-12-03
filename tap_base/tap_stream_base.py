"""TapStreamBase abstract class."""

import abc  # abstract base classes
import datetime
import time
from typing import Dict, Iterable, Any, Optional

import singer
from singer import CatalogEntry, StateMessage, RecordMessage

from tap_base.connection_base import GenericConnectionBase


class TapStreamBase(metaclass=abc.ABCMeta):
    """Abstract base class for tap streams."""

    _id: str
    _catalog_entry: CatalogEntry
    _state: StateMessage
    _latest_state: StateMessage

    def __init__(
        self,
        tap_stream_id: str,
        connection: GenericConnectionBase,
        catalog_entry: CatalogEntry = None,
        state: StateMessage = None,
    ):
        """Initialize tap stream."""
        self._id = tap_stream_id
        self._conn = connection
        self._state = state
        self._catalog_entry = catalog_entry

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

    def set_replication_method_and_key(self, stream, r_method, r_key):
        new_md = singer.metadata.to_map(stream.metadata)
        old_md = new_md.get(())
        if r_method:
            old_md.update({"replication-method": r_method})

        if r_key:
            old_md.update({"replication-key": r_key})

        stream.metadata = singer.metadata.to_list(new_md)
        return stream

    def sync(self):
        # columns = self.get_column_names()
        for row_dict in self.get_row_generator():
            record = self.transform_row_to_singer_record(row=row_dict)
            print(record)

    @abc.abstractmethod
    def get_row_generator(self) -> Iterable[Iterable[Any]]:
        pass

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
        """Transform SQL row to singer compatible record message."""
        rec: Dict[str, Any] = {}
        for property_name, elem in row.items():
            if True:
                # TODO: Debug this
                property_type = "Unknown"
            elif not self._catalog_entry.schema:
                property_type = "Unknown"
            else:
                property_type = self._catalog_entry.schema.properties[
                    property_name
                ].type
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
