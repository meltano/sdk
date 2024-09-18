from __future__ import annotations

import csv
import typing as t

from singer_sdk.contrib.filesystem import FileStream

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Record


class CSVStream(FileStream):
    """CSV stream class."""

    def read_file(self, path: str) -> t.Iterable[Record]:
        with self.filesystem.open(path, mode="r") as file:
            reader = csv.DictReader(
                file,
                delimiter=self.config["delimiter"],
                quotechar=self.config["quotechar"],
                escapechar=self.config.get("escapechar"),
                doublequote=self.config["doublequote"],
                lineterminator=self.config["lineterminator"],
            )
            yield from reader
