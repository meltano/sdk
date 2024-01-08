"""A simple tap with one big record and schema."""

from __future__ import annotations

import json
import sys

from singer_sdk import Stream, Tap

if sys.version_info < (3, 9):
    import importlib_resources
else:
    import importlib.resources as importlib_resources

PROJECT_DIR = importlib_resources.files("samples.aapl")


class AAPL(Stream):
    """An AAPL stream."""

    name = "aapl"
    schema_filepath = PROJECT_DIR / "fundamentals.json"

    def get_records(self, _):
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
