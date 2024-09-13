"""Sample Tap for CSV files."""  # noqa: INP001

from __future__ import annotations

import enum
import os

import singer_sdk.typing as th
from singer_sdk import Tap


def file_path_to_stream_name(file_path: str) -> str:
    """Convert a file path to a stream name."""
    return os.path.basename(file_path).replace(".csv", "").replace(os.sep, "__")  # noqa: PTH119


class ReadMode(str, enum.Enum):
    """Sync mode for the tap."""

    one_stream_per_file = "one_stream_per_file"
    merge = "merge"


class SampleTapCSV(Tap):
    """Sample Tap for CSV files."""

    name = "sample-tap-countries"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "path",
            th.StringType,
            required=True,
            description="Path to CSV files.",
        ),
        th.Property(
            "read_mode",
            th.StringType,
            required=True,
            description=(
                "Use `one_stream_per_file` to read each file as a separate stream, or "
                "`merge` to merge all files into a single stream."
            ),
            allowed_values=[ReadMode.one_stream_per_file, ReadMode.merge],
        ),
        th.Property(
            "stream_name",
            th.StringType,
            required=False,
            description="Name of the stream to use when `read_mode` is `merge`.",
        ),
        # TODO(edgarmondragon): Other configuration options.
    ).to_dict()

    def discover_streams(self) -> list:
        # TODO(edgarmondragon): Implement stream discovery, based on the configured path
        # and read mode.
        raise NotImplementedError
