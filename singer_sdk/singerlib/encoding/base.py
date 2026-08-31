"""Alias for :mod:`singer.encoding.base`."""

from __future__ import annotations

from singer.encoding.base import (
    GenericSingerReader,
    GenericSingerWriter,
    SingerMessageType,
)

__all__ = [
    "GenericSingerReader",
    "GenericSingerWriter",
    "SingerMessageType",
]
