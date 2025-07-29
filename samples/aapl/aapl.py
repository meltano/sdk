"""A simple tap with one big record and schema."""

from __future__ import annotations

import importlib.resources
import json

from singer_sdk import SchemaDirectory, Stream, StreamSchema, Tap

PROJECT_DIR = SchemaDirectory(importlib.resources.files("samples.aapl"))


class AAPL(Stream):
    """An AAPL stream."""

    name = "aapl"
    schema = StreamSchema(PROJECT_DIR, key="fundamentals")

    def get_records(self, _):  # noqa: PLR6301
        """Generate a single record."""
        with PROJECT_DIR.joinpath("AAPL.json").open() as f:
            record = json.load(f)

        yield record


class Fundamentals(Tap):
    """Singer tap for fundamentals."""

    name = "fundamentals"

    def discover_streams(self):
        """Get financial streams."""
        return [AAPL(self)]
