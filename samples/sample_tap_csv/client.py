from __future__ import annotations

import csv
import datetime
import os
import typing as t

from singer_sdk import Stream
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

    @property
    def partitions(self) -> list[Context]:
        return self._partitions

    def _read_file(self, path: str) -> t.Iterable[Record]:  # noqa: PLR6301
        # Make these configurable.
        delimiter = ","
        quotechar = '"'
        escapechar = None
        doublequote = True
        lineterminator = "\r\n"

        # TODO: Use filesytem-specific file open method.
        with open(path, encoding="utf-8") as file:  # noqa: PTH123
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
        mtime = os.path.getmtime(path)  # noqa: PTH204

        if (
            self.replication_method is REPLICATION_INCREMENTAL
            and (previous_bookmark := self.get_starting_timestamp(context))
            and _to_datetime(mtime) < previous_bookmark
        ):
            self.logger.info("File has not been modified since last read, skipping")
            return

        for record in self._read_file(path):
            record[SDC_META_MODIFIED_AT] = _to_datetime(mtime)
            yield record
