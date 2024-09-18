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


class ReadMode(str, enum.Enum):
    """Sync mode for the tap."""

    one_stream_per_file = "one_stream_per_file"
    merge = "merge"


BASE_CONFIG_SCHEMA = th.PropertiesList(
    th.Property(
        "filesystem",
        th.StringType,
        required=True,
        default="local",
        allowed_values=["local"],
        description="The filesystem to use.",
    ),
    th.Property(
        "path",
        th.StringType,
        required=True,
        description="Path to the directory where the files are stored.",
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
).to_dict()


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


_T = t.TypeVar("_T", bound=FileStream)


class FolderTap(Tap, t.Generic[_T]):
    """Singer tap for files in a directory."""

    valid_extensions: tuple[str, ...]

    default_stream_class: type[_T]

    config_jsonschema: t.ClassVar[dict] = {"properties": {}}

    @classmethod
    def append_builtin_config(cls: type[FolderTap], config_jsonschema: dict) -> None:
        """Appends built-in config to `config_jsonschema` if not already set.

        To customize or disable this behavior, developers may either override this class
        method or override the `capabilities` property to disabled any unwanted
        built-in capabilities.

        For all except very advanced use cases, we recommend leaving these
        implementations "as-is", since this provides the most choice to users and is
        the most "future proof" in terms of taking advantage of built-in capabilities
        which may be added in the future.

        Args:
            config_jsonschema: [description]
        """

        def _merge_missing(source_jsonschema: dict, target_jsonschema: dict) -> None:
            # Append any missing properties in the target with those from source.
            for k, v in source_jsonschema["properties"].items():
                if k not in target_jsonschema["properties"]:
                    target_jsonschema["properties"][k] = v

        _merge_missing(BASE_CONFIG_SCHEMA, config_jsonschema)

        super().append_builtin_config(config_jsonschema)

    @functools.cached_property
    def read_mode(self) -> ReadMode:
        """Folder read mode."""
        return ReadMode(self.config["read_mode"])

    @functools.cached_property
    def fs(self) -> fsspec.AbstractFileSystem:
        """Return the filesystem object."""
        return fsspec.filesystem(self.config["filesystem"])

    def discover_streams(self) -> list:
        """Return a list of discovered streams.

        Raises:
            ValueError: If the path does not exist or is not a directory.
        """
        # A directory for now, but could be a glob pattern.
        path: str = self.config["path"]

        if not self.fs.exists(path) or not self.fs.isdir(path):  # pragma: no cover
            # Raise a more specific error if the path is not a directory.
            msg = f"Path {path} does not exist or is not a directory"
            raise ValueError(msg)

        # One stream per file
        if self.read_mode == ReadMode.one_stream_per_file:
            return [
                self.default_stream_class(
                    tap=self,
                    name=file_path_to_stream_name(member),
                    filesystem=self.fs,
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
                filesystem=self.fs,
                partitions=contexts,
            )
        ]
