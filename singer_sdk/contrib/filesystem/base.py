"""Abstract classes for file system operations."""

from __future__ import annotations

import abc
import enum
import io
import typing as t

if t.TYPE_CHECKING:
    import datetime
    import types

__all__ = ["AbstractFile", "AbstractFileSystem"]

_F = t.TypeVar("_F")


class FileMode(str, enum.Enum):
    read = "rb"
    write = "wb"


class AbstractFile(abc.ABC):
    """Abstract class for file operations."""

    def __init__(self, buffer: io.BytesIO, filename: str):
        """Create a new AbstractFile instance."""
        self.buffer = buffer
        self.filename = filename

    def read(self, size: int = -1) -> bytes:
        """Read the file contents.

        Args:
            size: The number of bytes to read. If -1, read the entire file.

        Returns:
            The file contents as bytes.
        """
        return self.buffer.read(size)

    def write(self, data: bytes) -> int:
        """Write data to the file.

        Args:
            data: The data to write.

        Returns:
            The number of bytes written.
        """
        return self.buffer.write(data)

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        """Seek to a position in the file.

        Args:
            offset: The offset to seek to.
            whence: The reference point for the offset.

        Returns:
            The new position in the file.
        """
        return self.buffer.seek(offset, whence)

    def tell(self) -> int:
        """Get the current position in the file.

        Returns:
            The current position in the file.
        """
        return self.buffer.tell()

    def __enter__(self: _F) -> _F:
        """Enter the context manager.

        Returns:
            The file object.
        """
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: types.TracebackType | None,
    ) -> None:
        """Close the file.

        Args:
            exc_type: The exception type.
            exc_value: The exception value.
            traceback: The traceback.
        """
        self.close()

    @abc.abstractmethod
    def close(self) -> None:
        """Close the file."""

    @abc.abstractmethod
    def __iter__(self) -> t.Iterator[str]:
        """Iterate over the file contents as lines."""

    @abc.abstractmethod
    def seekable(self) -> bool:
        """Whether the file is seekable."""


class AbstractFileSystem(abc.ABC, t.Generic[_F]):
    """Abstract class for file system operations."""

    @abc.abstractmethod
    def open(self, filename: str, mode: str, newline: str, encoding: str) -> _F:
        """Open a file."""

    @abc.abstractmethod
    def modified(self, filename: str) -> datetime.datetime:
        """Get the last modified time of a file."""

    @abc.abstractmethod
    def created(self, filename: str) -> datetime.datetime:
        """Get the creation time of a file."""
