"""Stream class for CSV files."""

from __future__ import annotations

import sys
import typing as t

import pyarrow.csv

from singer_sdk.contrib.filesystem import FileStream
from singer_sdk.contrib.filesystem.stream import SDC_META_FILEPATH

if sys.version_info >= (3, 12):
    from typing import override  # noqa: ICN003
else:
    from typing_extensions import override

if t.TYPE_CHECKING:
    import pyarrow as pa

    from singer_sdk.helpers.types import Record


SDC_META_LINE_NUMBER = "_sdc_line_number"

# Mapping from PyArrow types to JSON Schema types
PYARROW_TO_JSONSCHEMA: dict[str, dict[str, t.Any]] = {
    "int8": {"type": "integer"},
    "int16": {"type": "integer"},
    "int32": {"type": "integer"},
    "int64": {"type": "integer"},
    "uint8": {"type": "integer"},
    "uint16": {"type": "integer"},
    "uint32": {"type": "integer"},
    "uint64": {"type": "integer"},
    "float16": {"type": "number"},
    "float32": {"type": "number"},
    "float64": {"type": "number"},
    "double": {"type": "number"},
    "bool": {"type": "boolean"},
    "string": {"type": "string"},
    "large_string": {"type": "string"},
    "date32": {"type": "string", "format": "date"},
    "date64": {"type": "string", "format": "date"},
    "timestamp": {"type": "string", "format": "date-time"},
}


def pyarrow_type_to_jsonschema(
    pa_type: pa.DataType,
    *,
    nullable: bool = False,
) -> dict[str, t.Any]:
    """Convert a PyArrow type to a JSON Schema type."""
    type_str = str(pa_type)
    # Handle parameterized types like timestamp[us, tz=UTC]
    base_type = type_str.split("[", maxsplit=1)[0]
    schema = PYARROW_TO_JSONSCHEMA.get(base_type, {"type": "string"}).copy()

    if nullable:
        # Convert {"type": "string"} to {"type": ["string", "null"]}
        schema["type"] = [schema["type"], "null"]

    return schema


class CSVStream(FileStream):
    """CSV stream class."""

    primary_keys = (SDC_META_FILEPATH, SDC_META_LINE_NUMBER)

    def _get_parse_options(self) -> pyarrow.csv.ParseOptions:
        """Return PyArrow parse options based on config."""
        return pyarrow.csv.ParseOptions(
            delimiter=self.config["delimiter"],
            quote_char=self.config["quotechar"],
            escape_char=self.config.get("escapechar"),  # type: ignore[arg-type]
            double_quote=self.config["doublequote"],
        )

    @override
    def get_schema(self, path: str) -> dict[str, t.Any]:
        """Return a schema for the given file."""
        with self.filesystem.open(path, mode="rb") as file:
            # Use open_csv for streaming - it infers schema from the first block
            # without reading the entire file
            reader = pyarrow.csv.open_csv(file, parse_options=self._get_parse_options())
            schema: dict[str, t.Any] = {
                "type": "object",
                "properties": {
                    field.name: pyarrow_type_to_jsonschema(
                        field.type,
                        nullable=field.nullable,
                    )
                    for field in reader.schema
                },
            }
            schema["properties"][SDC_META_LINE_NUMBER] = {"type": "integer"}
            return schema

    @override
    def read_file(self, path: str) -> t.Iterable[Record]:
        """Read the given file and emit records."""
        with self.filesystem.open(path, mode="rb") as file:
            # Use streaming reader to avoid loading entire file into memory
            reader = pyarrow.csv.open_csv(file, parse_options=self._get_parse_options())
            line_number = 2  # Line 1 is the header
            for batch in reader:
                for record in batch.to_pylist():
                    record[SDC_META_LINE_NUMBER] = line_number
                    line_number += 1
                    yield record
