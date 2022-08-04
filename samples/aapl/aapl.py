"""A simple tap with one big record and schema."""

import json

from samples import aapl
from singer_sdk import Stream, Tap
from singer_sdk.helpers._util import get_package_files

PROJECT_DIR = get_package_files(aapl)


class AAPL(Stream):
    """An AAPL stream."""

    name = "aapl"
    schema_filepath = PROJECT_DIR / "fundamentals.json"

    def get_records(self, _):
        """Generate a single record."""
        with open(PROJECT_DIR / "AAPL.json") as f:
            record = json.load(f)

        yield record


class Fundamentals(Tap):
    """Singer tap for fundamentals."""

    name = "fundamentals"

    def discover_streams(self):
        """Get financial streams."""
        return [AAPL(self)]
