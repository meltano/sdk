"""Local filesystem operations."""

from __future__ import annotations

import typing as t
from datetime import datetime
from pathlib import Path

from singer_sdk.contrib.filesystem import base

if t.TYPE_CHECKING:
    import contextlib

__all__ = ["LocalFile", "LocalFileSystem"]


class LocalFile(base.AbstractFile):
    """Local file operations."""

    def close(self) -> None:
        """Close the file."""
        return self.buffer.close()

    def seekable(self) -> bool:  # noqa: D102, PLR6301
        return True

    def __iter__(self) -> contextlib.Iterator[str]:  # noqa: D105
        return iter(self.buffer)


class LocalFileSystem(base.AbstractFileSystem[LocalFile]):
    """Local filesystem operations."""

    def __init__(self, root: str) -> None:
        """Create a new LocalFileSystem instance."""
        self._root_path = Path(root).absolute()

    @property
    def root(self) -> Path:
        """Get the root path."""
        return self._root_path

    def open(  # noqa: D102
        self,
        filename: str,
        *,
        mode: base.FileMode = base.FileMode.read,
    ) -> LocalFile:
        filepath = self.root / filename
        return LocalFile(filepath.open(mode=mode), filename)

    def modified(self, filename: str) -> datetime:  # noqa: D102
        stat = (self.root / filename).stat()
        return datetime.fromtimestamp(stat.st_mtime).astimezone()

    def created(self, filename: str) -> datetime:  # noqa: D102
        stat = (self.root / filename).stat()
        try:
            return datetime.fromtimestamp(stat.st_birthtime).astimezone()  # type: ignore[attr-defined]
        except AttributeError:
            return datetime.fromtimestamp(stat.st_ctime).astimezone()
