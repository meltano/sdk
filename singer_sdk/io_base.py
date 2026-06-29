"""Abstract base classes for all Singer messages IO operations."""

from __future__ import annotations

__lazy_modules__ = {"singer_sdk.singerlib", "singer_sdk.singerlib.encoding"}

from singer_sdk.singerlib.encoding import (
    GenericSingerReader,
    GenericSingerWriter,
    SingerMessageType,
)
from singer_sdk.singerlib.encoding import SimpleSingerReader as SingerReader
from singer_sdk.singerlib.encoding import SimpleSingerWriter as SingerWriter

__all__ = [
    "GenericSingerReader",
    "GenericSingerWriter",
    "SingerMessageType",
    "SingerReader",
    "SingerWriter",
]
