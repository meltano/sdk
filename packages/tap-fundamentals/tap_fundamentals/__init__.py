"""A simple tap with one big record and schema."""

from __future__ import annotations

import importlib.resources
import json
import sys
import typing as t

from singer_sdk import SchemaDirectory, Stream, StreamSchema, Tap

from . import data

if sys.version_info >= (3, 12):
    from typing import override  # noqa: ICN003
else:
    from typing_extensions import override

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context

PROJECT_DIR = importlib.resources.files(data)
SCHEMA_DIR = SchemaDirectory(PROJECT_DIR)


class AAPL(Stream):
    """An AAPL stream."""

    name = "aapl"
    schema: t.ClassVar[StreamSchema] = StreamSchema(SCHEMA_DIR, key="fundamentals")

    @override
    def get_records(
        self,
        context: Context | None = None,
    ) -> t.Iterator[dict[str, t.Any]]:
        """Generate a single record."""
        with PROJECT_DIR.joinpath("AAPL.json").open() as f:
            record = json.load(f)

        yield record


class Fundamentals(Tap):
    """Singer tap for fundamentals."""

    name = "fundamentals"

    @override
    def discover_streams(self) -> list[AAPL]:
        """Get financial streams."""
        return [AAPL(self)]
