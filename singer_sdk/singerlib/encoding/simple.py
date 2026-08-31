"""Alias for :mod:`singer.encoding.simple`."""

from __future__ import annotations

from singer.encoding.simple import (
    ActivateVersionMessage,
    Message,
    RecordMessage,
    SchemaMessage,
    SimpleSingerReader,
    SimpleSingerWriter,
    StateMessage,
)

__all__ = [
    "ActivateVersionMessage",
    "Message",
    "RecordMessage",
    "SchemaMessage",
    "SimpleSingerReader",
    "SimpleSingerWriter",
    "StateMessage",
]
