"""Singer tap for files in a directory."""

from __future__ import annotations

import enum
import functools
import logging
import typing as t
from pathlib import Path

import fsspec
import fsspec.implementations
import fsspec.implementations.dirfs

import singer_sdk.typing as th
from singer_sdk import Tap
from singer_sdk.contrib.filesystem import config as filesystem_config
from singer_sdk.contrib.filesystem.stream import FileStream
from singer_sdk.exceptions import ConfigValidationError

logger = logging.getLogger(__name__)

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
        allowed_values=["local", "ftp", "sftp"],
        title="Filesystem",
        description="The filesystem to use.",
    ),
    th.Property(
        "path",
        th.StringType,
        required=True,
        title="Directory Path",
        description="Path to the directory where the files are stored.",
    ),
    th.Property(
        "read_mode",
        th.StringType,
        required=True,
        title="Read Mode",
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
        title="Stream Name (Merge Mode)",
        description="Name of the stream to use when `read_mode` is `merge`.",
    ),
    filesystem_config.FTP,
    filesystem_config.SFTP,
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
    """Valid file extensions for this tap.

    Files with extensions not in this list will be ignored.
    """

    default_stream_class: type[_T]
    """The default stream class to use for this tap.

    This should be a subclass of `FileStream`.
    """

    dynamic_catalog: bool = True

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

        def _merge_missing(src: dict, tgt: dict) -> None:
            # Append any missing properties in the target with those from source.
            for k, v in src["properties"].items():
                if k not in tgt["properties"]:
                    tgt["properties"][k] = v

                # Merge the required fields
                source_required = src.get("required", [])
                target_required = tgt.get("required", [])
                tgt["required"] = list(set(source_required + target_required))

        _merge_missing(BASE_CONFIG_SCHEMA, config_jsonschema)

        super().append_builtin_config(config_jsonschema)

    @functools.cached_property
    def read_mode(self) -> ReadMode:
        """Folder read mode."""
        return ReadMode(self.config["read_mode"])

    @functools.cached_property
    def path(self) -> str:
        """Return the path to the directory."""
        return self.config["path"]  # type: ignore[no-any-return]

    @functools.cached_property
    def fs(self) -> fsspec.AbstractFileSystem:
        """Return the filesystem object.

        Raises:
            ConfigValidationError: If the filesystem configuration is missing.
        """
        protocol = self.config["filesystem"]
        if protocol != "local" and protocol not in self.config:  # pragma: no cover
            msg = "Filesystem configuration is missing"
            raise ConfigValidationError(
                msg,
                errors=[f"Missing configuration for filesystem {protocol}"],
            )
        logger.info("Instantiating filesystem interface: '%s'", protocol)

        return fsspec.implementations.dirfs.DirFileSystem(
            path=self.path,
            target_protocol=protocol,
            target_options=self.config.get(protocol),
        )

    def discover_streams(self) -> list:
        """Return a list of discovered streams.

        Raises:
            ValueError: If the path does not exist or is not a directory.
        """
        # A directory for now, but could be a glob pattern.
        if not self.fs.exists(".") or not self.fs.isdir("."):  # pragma: no cover
            # Raise a more specific error if the path is not a directory.
            msg = f"Path {self.path} does not exist or is not a directory"
            raise ValueError(msg)

        # One stream per file
        if self.read_mode == ReadMode.one_stream_per_file:
            return [
                self.default_stream_class(
                    tap=self,
                    name=file_path_to_stream_name(member["name"]),
                    filepaths=[member["name"]],
                    filesystem=self.fs,
                )
                for member in self.fs.listdir(".")
                if member["type"] == "file"
                and member["name"].endswith(self.valid_extensions)
            ]

        # Merge
        return [
            self.default_stream_class(
                tap=self,
                name=self.config["stream_name"],
                filepaths=[
                    member["name"]
                    for member in self.fs.listdir(".")
                    if member["type"] == "file"
                    and member["name"].endswith(self.valid_extensions)
                ],
                filesystem=self.fs,
            )
        ]
