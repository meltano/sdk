"""Sample Tap for CSV files."""

from __future__ import annotations

import enum
import functools
import os

import singer_sdk.typing as th
from samples.sample_tap_csv.client import SDC_META_FILEPATH, CSVStream
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
            allowed_values=[
                ReadMode.one_stream_per_file,
                ReadMode.merge,
            ],
        ),
        th.Property(
            "stream_name",
            th.StringType,
            required=False,
            description="Name of the stream to use when `read_mode` is `merge`.",
        ),
        # TODO(edgarmondragon): Other configuration options.
    ).to_dict()

    @functools.cached_property
    def read_mode(self) -> ReadMode:
        return ReadMode(self.config["read_mode"])

    def discover_streams(self) -> list:
        # TODO(edgarmondragon): Implement stream discovery, based on the configured path
        # and read mode.
        path: str = self.config[
            "path"
        ]  # a directory for now, but could be a glob pattern

        # One stream per file
        if self.read_mode == ReadMode.one_stream_per_file:
            if os.path.isdir(path):  # noqa: PTH112
                return [
                    CSVStream(
                        tap=self,
                        name=file_path_to_stream_name(member),
                        partitions=[{SDC_META_FILEPATH: os.path.join(path, member)}],  # noqa: PTH118
                    )
                    for member in os.listdir(path)
                    if member.endswith(".csv")
                ]

            msg = f"Path {path} is not a directory."
            raise ValueError(msg)

        # Merge
        if os.path.isdir(path):  # noqa: PTH112
            contexts = [
                {
                    SDC_META_FILEPATH: os.path.join(path, member),  # noqa: PTH118
                }
                for member in os.listdir(path)
                if member.endswith(".csv")
            ]
            return [
                CSVStream(
                    tap=self,
                    name=self.config.get("stream_name", "merged_stream"),
                    partitions=contexts,
                )
            ]
        return []
