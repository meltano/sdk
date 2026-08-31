"""Alias for :mod:`singer.messages`."""

from __future__ import annotations

from singer.messages import (
    WRITER,
    ActivateVersionMessage,
    Message,
    RecordMessage,
    SchemaMessage,
    SingerMessageType,
    SingerWriter,
    StateMessage,
    exclude_null_dict,
    format_message,
    write_message,
    write_record,
    write_schema,
    write_state,
    write_version,
)

__all__ = [
    "WRITER",
    "ActivateVersionMessage",
    "Message",
    "RecordMessage",
    "SchemaMessage",
    "SingerMessageType",
    "SingerWriter",
    "StateMessage",
    "exclude_null_dict",
    "format_message",
    "write_message",
    "write_record",
    "write_schema",
    "write_state",
    "write_version",
]
