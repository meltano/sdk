"""Singer message types and utilities."""

from __future__ import annotations

from singer_sdk._singerlib.encoding import SingerWriter
from singer_sdk._singerlib.encoding.base import (
    ActivateVersionMessage,
    Message,
    RecordMessage,
    SchemaMessage,
    SingerMessageType,
    StateMessage,
    exclude_null_dict,
)

__all__ = [
    "ActivateVersionMessage",
    "Message",
    "RecordMessage",
    "SchemaMessage",
    "SingerMessageType",
    "StateMessage",
    "exclude_null_dict",
    "format_message",
    "write_message",
]

WRITER = SingerWriter()
format_message = WRITER.format_message
write_message = WRITER.write_message
