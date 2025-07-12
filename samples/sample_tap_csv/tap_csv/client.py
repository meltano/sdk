"""Stream class for CSV files."""

from __future__ import annotations

import csv
import typing as t

from singer_sdk.contrib.filesystem import FileStream
from singer_sdk.contrib.filesystem.stream import SDC_META_FILEPATH

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Record


SDC_META_LINE_NUMBER = "_sdc_line_number"


class CSVStream(FileStream):
    """CSV stream class."""

    @property
    def primary_keys(self) -> t.Sequence[str]:
        """Return the primary key fields for records in this stream."""
        return (SDC_META_FILEPATH, SDC_META_LINE_NUMBER)

    def get_schema(self, path: str) -> dict[str, t.Any]:
        """Return a schema for the given file."""
        with self.filesystem.open(path, mode="r") as file:
            reader = csv.DictReader(
                file,
                delimiter=self.config["delimiter"],
                quotechar=self.config["quotechar"],
                escapechar=self.config.get("escapechar"),
                doublequote=self.config["doublequote"],
                lineterminator=self.config["lineterminator"],
            )
            schema = {
                "type": "object",
                "properties": {key: {"type": "string"} for key in reader.fieldnames},
            }
            schema["properties"][SDC_META_LINE_NUMBER] = {"type": "integer"}
            return schema

    def read_file(self, path: str) -> t.Iterable[Record]:
        """Read the given file and emit records."""
        with self.filesystem.open(path, mode="r") as file:
            reader = csv.DictReader(
                file,
                delimiter=self.config["delimiter"],
                quotechar=self.config["quotechar"],
                escapechar=self.config.get("escapechar"),
                doublequote=self.config["doublequote"],
                lineterminator=self.config["lineterminator"],
            )
            for record in reader:
                record[SDC_META_LINE_NUMBER] = reader.line_num
                yield record
