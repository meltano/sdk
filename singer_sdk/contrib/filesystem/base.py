"""Abstract classes for file system operations."""

from __future__ import annotations

import abc
import typing as t

__all__ = ["AbstractDirectory", "AbstractFile", "AbstractFileSystem"]


class AbstractFile(abc.ABC):
    """Abstract class for file operations."""

    @abc.abstractmethod
    def read(self, size: int = -1) -> str:
        """Read the file contents."""

    def read_text(self) -> str:
        """Read the entire file as text.

        Returns:
            The file contents as a string.
        """
        return self.read()


_F = t.TypeVar("_F")
_D = t.TypeVar("_D")


class AbstractDirectory(abc.ABC, t.Generic[_F]):
    """Abstract class for directory operations."""

    @abc.abstractmethod
    def list_contents(self: _D) -> t.Generator[_F | _D, None, None]:
        """List files in the directory.

        Yields:
            A file or directory node
        """
        yield self
        yield from []


class AbstractFileSystem(abc.ABC):
    """Abstract class for file system operations."""

    @abc.abstractmethod
    def open(self, path: str) -> AbstractFile:
        """Open a file for reading."""
