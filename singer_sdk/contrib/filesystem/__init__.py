"""Filesystem interfaces for the Singer SDK."""

from __future__ import annotations

from singer_sdk.contrib.filesystem.stream import FileStream
from singer_sdk.contrib.filesystem.tap import FolderTap

__all__ = ["FileStream", "FolderTap"]
