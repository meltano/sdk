"""Singer message types and utilities."""

from __future__ import annotations

import typing as t

from .encoding import SimpleSingerWriter as SingerWriter
from .encoding.base import SingerMessageType
from .encoding.simple import (
    ActivateVersionMessage,
    Message,
    RecordMessage,
    SchemaMessage,
    StateMessage,
    exclude_null_dict,
)

if t.TYPE_CHECKING:
    from collections.abc import Mapping
    from datetime import datetime

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
    "write_record",
    "write_schema",
    "write_state",
    "write_version",
]

WRITER = SingerWriter()
format_message = WRITER.format_message
write_message = WRITER.write_message


def write_record(
    stream_name: str,
    record: dict[str, t.Any],
    *,
    version: int | None = None,
    time_extracted: datetime | None = None,
) -> None:
    """Write a RECORD message to stdout.

    Args:
        stream_name: The stream name.
        record: The record data.
        version: The record version.
        time_extracted: The time the record was extracted.
    """
    write_message(
        RecordMessage(
            stream=stream_name,
            record=record,
            version=version,
            time_extracted=time_extracted,
        ),
    )


def write_schema(
    stream_name: str,
    schema: dict[str, t.Any],
    *,
    key_properties: list[str] | None = None,
    bookmark_properties: list[str] | None = None,
) -> None:
    """Write a SCHEMA message to stdout.

    Args:
        stream_name: The stream name.
        schema: The JSON schema.
        key_properties: The key properties.
        bookmark_properties: The bookmark properties.
    """
    write_message(
        SchemaMessage(
            stream=stream_name,
            schema=schema,
            key_properties=key_properties,
            bookmark_properties=bookmark_properties,
        ),
    )


def write_state(value: Mapping[str, t.Any]) -> None:
    """Write a STATE message to stdout.

    Args:
        value: The state value.
    """
    write_message(StateMessage(value=value))


def write_version(stream_name: str, version: int) -> None:
    """Write an ACTIVATE_VERSION message to stdout.

    Args:
        stream_name: The stream name.
        version: The version to activate.
    """
    write_message(ActivateVersionMessage(stream=stream_name, version=version))
