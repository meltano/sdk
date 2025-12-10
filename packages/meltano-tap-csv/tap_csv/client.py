"""Stream class for CSV files."""

from __future__ import annotations

import csv
import sys
import typing as t

from singer_sdk.contrib.filesystem import FileStream
from singer_sdk.contrib.filesystem.stream import SDC_META_FILEPATH

if sys.version_info >= (3, 12):
    from typing import override  # noqa: ICN003
else:
    from typing_extensions import override

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Record


SDC_META_LINE_NUMBER = "_sdc_line_number"


class CSVStream(FileStream):
    """CSV stream class."""

    primary_keys = (SDC_META_FILEPATH, SDC_META_LINE_NUMBER)

    @override
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
            if reader.fieldnames is not None:
                schema: dict[str, t.Any] = {
                    "type": "object",
                    "properties": {
                        key: {"type": "string"} for key in reader.fieldnames
                    },
                }
                schema["properties"][SDC_META_LINE_NUMBER] = {"type": "integer"}
            else:
                schema = {
                    "type": "object",
                    "properties": {
                        SDC_META_LINE_NUMBER: {"type": "integer"},
                    },
                    "additionalProperties": True,
                }
            return schema

    @override
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
