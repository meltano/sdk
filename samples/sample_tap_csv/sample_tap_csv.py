"""Sample Tap for CSV files."""

from __future__ import annotations

from singer_sdk.contrib.filesystem import FolderTap


class SampleTapCSV(FolderTap):
    """Sample Tap for CSV files."""

    name = "sample-tap-csv"
    valid_extensions: tuple[str, ...] = (".csv",)
