"""Local filesystem operations."""

from __future__ import annotations

import pathlib
import typing as t

from singer_sdk.contrib.filesystem import base

__all__ = ["LocalDirectory", "LocalFile"]


class LocalFile(base.AbstractFile):
    """Local file operations."""

    def __init__(self, filepath: str | pathlib.Path):
        """Create a new LocalFile instance."""
        self._filepath = filepath
        self.path = pathlib.Path(self._filepath).absolute()

    def __repr__(self) -> str:
        """A string representation of the LocalFile.

        Returns:
            A string representation of the LocalFile.
        """
        return f"LocalFile({self._filepath})"

    def read(self, size: int = -1) -> str:
        """Read the file contents.

        Args:
            size: Number of bytes to read. If not specified, the entire file is read.

        Returns:
            The file contents as a string.
        """
        with self.path.open("r") as file:
            return file.read(size)


class LocalDirectory(base.AbstractDirectory[t.Union[LocalFile, "LocalDirectory"]]):
    """Local directory operations."""

    def __init__(self, dirpath: str | pathlib.Path):
        """Create a new LocalDirectory instance."""
        self._dirpath = dirpath
        self.path = pathlib.Path(self._dirpath).absolute()

    def __repr__(self) -> str:
        """A string representation of the LocalDirectory.

        Returns:
            A string representation of the LocalDirectory.
        """
        return f"LocalDirectory({self._dirpath})"

    def list_contents(self) -> t.Generator[LocalFile | LocalDirectory, None, None]:
        """List files in the directory.

        Yields:
            A file or directory node
        """
        for child in self.path.iterdir():
            if child.is_dir():
                subdir = LocalDirectory(child)
                yield subdir
                yield from subdir.list_contents()
            else:
                yield LocalFile(child)
