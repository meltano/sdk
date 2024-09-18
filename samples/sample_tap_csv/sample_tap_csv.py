"""Sample Tap for CSV files."""

from __future__ import annotations

from samples.sample_tap_csv.client import CSVStream
from singer_sdk.contrib.filesystem import FolderTap


class SampleTapCSV(FolderTap):
    """Sample Tap for CSV files."""

    name = "sample-tap-csv"
    valid_extensions: tuple[str, ...] = (".csv",)
    default_stream_class = CSVStream
