"""Abstract classes for file system operations."""

from __future__ import annotations

import abc
import typing as t

if t.TYPE_CHECKING:
    import datetime

__all__ = ["AbstractDirectory", "AbstractFile", "AbstractFileSystem"]


class AbstractFile(abc.ABC):
    """Abstract class for file operations."""

    def read_text(self, *, encoding: str = "utf-8") -> str:
        """Read the entire file as text.

        Args:
            encoding: The text encoding to use.

        Returns:
            The file contents as a string.
        """
        return self.read().decode(encoding)

    @abc.abstractmethod
    def read(self, size: int = -1) -> bytes:
        """Read the file contents."""

    @property
    def creation_time(self) -> datetime.datetime:
        """Get the creation time of the file."""
        raise NotImplementedError

    @property
    def modified_time(self) -> datetime.datetime:
        """Get the last modified time of the file."""
        raise NotImplementedError


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


class AbstractFileSystem(abc.ABC, t.Generic[_F, _D]):
    """Abstract class for file system operations."""

    @property
    @abc.abstractmethod
    def root(self) -> _D:
        """Get the root path."""
        raise NotImplementedError