"""Local filesystem operations."""

from __future__ import annotations

import typing as t
from datetime import datetime
from pathlib import Path

from singer_sdk.contrib.filesystem import base

__all__ = ["LocalDirectory", "LocalFile", "LocalFileSystem"]


class LocalFile(base.AbstractFile):
    """Local file operations."""

    def __init__(self, filepath: str | Path):
        """Create a new LocalFile instance."""
        self._filepath = filepath
        self.path = Path(self._filepath).absolute()

    def __repr__(self) -> str:
        """A string representation of the LocalFile.

        Returns:
            A string representation of the LocalFile.
        """
        return f"LocalFile({self._filepath})"

    def read(self, size: int = -1) -> bytes:
        """Read the file contents.

        Args:
            size: Number of bytes to read. If not specified, the entire file is read.

        Returns:
            The file contents as a string.
        """
        with self.path.open("rb") as file:
            return file.read(size)

    @property
    def creation_time(self) -> datetime:
        """Get the creation time of the file.

        Returns:
            The creation time of the file.
        """
        stat = self.path.stat()
        try:
            return datetime.fromtimestamp(stat.st_birthtime).astimezone()  # type: ignore[attr-defined]
        except AttributeError:
            return datetime.fromtimestamp(stat.st_ctime).astimezone()

    @property
    def modified_time(self) -> datetime:
        """Get the last modified time of the file.

        Returns:
            The last modified time of the file.
        """
        return datetime.fromtimestamp(self.path.stat().st_mtime).astimezone()


class LocalDirectory(base.AbstractDirectory[LocalFile]):
    """Local directory operations."""

    def __init__(self, dirpath: str | Path):
        """Create a new LocalDirectory instance."""
        self._dirpath = dirpath
        self.path = Path(self._dirpath).absolute()

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


class LocalFileSystem(base.AbstractFileSystem[LocalFile, LocalDirectory]):
    """Local filesystem operations."""

    def __init__(self, root: str) -> None:
        """Create a new LocalFileSystem instance."""
        self._root_dir = LocalDirectory(root)

    @property
    def root(self) -> LocalDirectory:
        """Get the root path."""
        return self._root_dir
