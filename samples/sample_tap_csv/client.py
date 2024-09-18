from __future__ import annotations

import csv
import typing as t

from singer_sdk.contrib.filesystem.stream import FileStream

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Record


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
