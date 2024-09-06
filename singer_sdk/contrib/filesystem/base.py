"""Abstract classes for file system operations."""

from __future__ import annotations

import abc
import typing as t


class AbstractFile(abc.ABC):
    """Abstract class for file operations."""

    @abc.abstractmethod
    def read(self) -> bytes:
        """Read the file contents."""


Node = t.Union[AbstractFile, "AbstractDirectory"]


class AbstractDirectory(abc.ABC):
    """Abstract class for directory operations."""

    @abc.abstractmethod
    def list_contents(self) -> t.Generator[Node, None, None]:
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
