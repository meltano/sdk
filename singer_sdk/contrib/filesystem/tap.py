"""Singer tap for files in a directory."""

from __future__ import annotations

import enum
import functools
import os
import typing as t
from pathlib import Path

import fsspec

import singer_sdk.typing as th
from singer_sdk import Tap
from singer_sdk.contrib.filesystem.stream import SDC_META_FILEPATH, FileStream

DEFAULT_MERGE_STREAM_NAME = "files"


def file_path_to_stream_name(file_path: str) -> str:
    """Convert a file path to a stream name.

    - Get rid of any extensions
    - Preserve the full path, but replace slashes with double underscores

    Args:
        file_path: The file path to convert.

    Returns:
        The stream name.
    """
    path_obj = Path(file_path)
    return path_obj.with_suffix("").as_posix().replace("/", "__")


class ReadMode(str, enum.Enum):
    """Sync mode for the tap."""

    one_stream_per_file = "one_stream_per_file"
    merge = "merge"


_T = t.TypeVar("_T", bound=FileStream)


class FolderTap(Tap, t.Generic[_T]):
    """Singer tap for files in a directory."""

    valid_extensions: tuple[str, ...]

    default_stream_class: type[_T]

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
            allowed_values=list(ReadMode),
        ),
        th.Property(
            "stream_name",
            th.StringType,
            required=True,
            default=DEFAULT_MERGE_STREAM_NAME,
            description="Name of the stream to use when `read_mode` is `merge`.",
        ),
        # TODO(edgarmondragon): Other configuration options.
    ).to_dict()

    @functools.cached_property
    def read_mode(self) -> ReadMode:
        """Folder read mode."""
        return ReadMode(self.config["read_mode"])

    def discover_streams(self) -> list:
        """Return a list of discovered streams.

        Raises:
            ValueError: If the path does not exist or is not a directory.
        """
        # TODO(edgarmondragon): Implement stream discovery, based on the configured path
        # and read mode.
        # A directory for now, but could be a glob pattern.
        path: str = self.config["path"]

        fs: fsspec.AbstractFileSystem = fsspec.filesystem("local")

        if not fs.exists(path) or not fs.isdir(path):  # pragma: no cover
            # Raise a more specific error if the path is not a directory.
            msg = f"Path {path} does not exist or is not a directory"
            raise ValueError(msg)

        # One stream per file
        if self.read_mode == ReadMode.one_stream_per_file:
            return [
                self.default_stream_class(
                    tap=self,
                    name=file_path_to_stream_name(member),
                    partitions=[{SDC_META_FILEPATH: os.path.join(path, member)}],  # noqa: PTH118
                )
                for member in os.listdir(path)
                if member.endswith(self.valid_extensions)
            ]

        # Merge
        contexts = [
            {
                SDC_META_FILEPATH: os.path.join(path, member),  # noqa: PTH118
            }
            for member in os.listdir(path)
            if member.endswith(self.valid_extensions)
        ]
        return [
            self.default_stream_class(
                tap=self,
                name=self.config["stream_name"],
                partitions=contexts,
            )
        ]
