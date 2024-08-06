"""Abstract base classes for all Singer messages IO operations."""

from __future__ import annotations

from singer_sdk._singerlib.encoding import (
    GenericSingerReader,
    GenericSingerWriter,
    SingerMessageType,
)
from singer_sdk._singerlib.encoding import SimpleSingerReader as SingerReader
from singer_sdk._singerlib.encoding import SimpleSingerWriter as SingerWriter

__all__ = [
    "GenericSingerReader",
    "GenericSingerWriter",
    "SingerMessageType",
    "SingerReader",
    "SingerWriter",
]
