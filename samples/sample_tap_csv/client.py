from __future__ import annotations

import abc
import csv
import typing as t

import fsspec

from singer_sdk import Stream
from singer_sdk.helpers._util import utc_now  # noqa: PLC2701
from singer_sdk.streams.core import REPLICATION_INCREMENTAL

if t.TYPE_CHECKING:
    import datetime

    from singer_sdk.helpers.types import Context, Record
    from singer_sdk.tap_base import Tap

SDC_META_FILEPATH = "_sdc_path"
SDC_META_MODIFIED_AT = "_sdc_modified_at"


class FileStream(Stream, metaclass=abc.ABCMeta):
    """Abstract base class for file streams."""

    def __init__(
        self,
        tap: Tap,
        name: str,
        *,
        partitions: list[Context] | None = None,
    ) -> None:
        # TODO(edgarmondragon): Build schema from CSV file.
        schema = {
            "type": ["object"],
            "properties": {
                SDC_META_FILEPATH: {"type": "string"},
                SDC_META_MODIFIED_AT: {"type": "string", "format": "date-time"},
            },
            "required": [],
            "additionalProperties": {"type": "string"},
        }
        super().__init__(tap, schema, name)

        # TODO(edgarrmondragon): Make this None if the filesytem does not support it.
        self.replication_key = SDC_META_MODIFIED_AT
        self._sync_start_time = utc_now()
        self.filesystem: fsspec.AbstractFileSystem = fsspec.filesystem("local")
        self._partitions = partitions or []

    @property
    def partitions(self) -> list[Context]:
        return self._partitions

    @abc.abstractmethod
    def read_file(self, context: Context | None) -> t.Iterable[Record]:
        """Return a generator of records from the file."""

    def get_records(
        self,
        context: Context | None,
    ) -> t.Iterable[Record | tuple[Record, Context | None]]:
        path: str = context[SDC_META_FILEPATH]

        mtime: datetime.datetime | None
        try:
            mtime: datetime.datetime = self.filesystem.modified(path)
        except NotImplementedError:
            self.logger.warning("Filesystem does not support modified time")
            mtime = None

        if (
            self.replication_method is REPLICATION_INCREMENTAL
            and (previous_bookmark := self.get_starting_timestamp(context))
            and mtime is not None
            and mtime < previous_bookmark
        ):
            self.logger.info("File has not been modified since last read, skipping")
            return

        for record in self.read_file(path):
            record[SDC_META_MODIFIED_AT] = mtime or self._sync_start_time
            yield record


class CSVStream(FileStream):
    """CSV stream class."""

    def read_file(self, path: str) -> t.Iterable[Record]:
        # Make these configurable.
        delimiter = ","
        quotechar = '"'
        escapechar = None
        doublequote = True
        lineterminator = "\r\n"

        with self.filesystem.open(path, mode="r") as file:
            reader = csv.DictReader(
                file,
                delimiter=delimiter,
                quotechar=quotechar,
                escapechar=escapechar,
                doublequote=doublequote,
                lineterminator=lineterminator,
            )
            yield from reader
