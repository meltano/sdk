"""Alias for :mod:`singer.encoding`."""

from __future__ import annotations

from singer.encoding import (
    GenericSingerReader,
    GenericSingerWriter,
    SimpleSingerReader,
    SimpleSingerWriter,
    SingerMessageType,
)

__all__ = [
    "GenericSingerReader",
    "GenericSingerWriter",
    "SimpleSingerReader",
    "SimpleSingerWriter",
    "SingerMessageType",
]
