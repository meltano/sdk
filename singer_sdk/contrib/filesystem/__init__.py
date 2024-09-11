"""Filesystem interfaces for the Singer SDK."""

from __future__ import annotations

from singer_sdk.contrib.filesystem import local, s3

__all__ = ["local", "s3"]
