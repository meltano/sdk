from __future__ import annotations

import csv
import datetime
import typing as t

import fsspec

from singer_sdk import Stream
from singer_sdk.helpers._util import utc_now  # noqa: PLC2701
from singer_sdk.streams.core import REPLICATION_INCREMENTAL

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context, Record
    from singer_sdk.tap_base import Tap

SDC_META_FILEPATH = "_sdc_path"
SDC_META_MODIFIED_AT = "_sdc_modified_at"


def _to_datetime(value: float) -> str:
    return datetime.datetime.fromtimestamp(value).astimezone()


class CSVStream(Stream):
    """CSV stream class."""

    def __init__(
        self,
        tap: Tap,
        name: str | None = None,
        *,
        partitions: list[str] | None = None,
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

        self._partitions = partitions or []

        self.filesystem: fsspec.AbstractFileSystem = fsspec.filesystem("local")
        self._sync_start_time = utc_now()

    @property
    def partitions(self) -> list[Context]:
        return self._partitions

    def _read_file(self, path: str) -> t.Iterable[Record]:
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

        for record in self._read_file(path):
            record[SDC_META_MODIFIED_AT] = mtime or self._sync_start_time
            yield record
