"""A simple tap with one big record and schema."""

from __future__ import annotations

import json
from pathlib import Path

from singer_sdk import Stream, Tap

PROJECT_DIR = Path(__file__).parent


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
