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
    exclude_null_dict,
)

__all__ = [
    "ActivateVersionMessage",
    "Message",
    "RecordMessage",
    "SchemaMessage",
    "SimpleSingerReader",
    "SimpleSingerWriter",
    "StateMessage",
    "exclude_null_dict",
]
