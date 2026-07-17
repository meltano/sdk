"""Convenience helpers matching the top-level ``singer-python`` API.

Kept out of ``singer/__init__.py`` (which only re-exports) and re-exported
from there.
"""

from __future__ import annotations

import typing as t

from singer import messages
from singer.messages import (
    ActivateVersionMessage,
    Message,
    RecordMessage,
    SchemaMessage,
    StateMessage,
)

if t.TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from datetime import datetime

__all__ = [
    "write_message",
    "write_record",
    "write_schema",
    "write_state",
    "write_version",
]


def write_message(message: Message) -> None:
    """Write a Singer message to stdout.

    Delegates to :func:`singer.messages.write_message`, so patching that
    function also affects callers of this one.

    Args:
        message: The message to write.
    """
    messages.write_message(message)


def write_record(
    stream_name: str,
    record: dict[str, t.Any],
    stream_alias: str | None = None,
    time_extracted: datetime | None = None,
) -> None:
    """Write a RECORD message to stdout.

    Args:
        stream_name: The stream name.
        record: The record data.
        stream_alias: An alias to use in place of the stream name.
        time_extracted: The time the record was extracted.
    """
    write_message(
        RecordMessage(
            stream=(stream_alias or stream_name),
            record=record,
            time_extracted=time_extracted,
        ),
    )


def write_schema(
    stream_name: str,
    schema: dict[str, t.Any],
    key_properties: Sequence[str],
    bookmark_properties: list[str] | None = None,
    stream_alias: str | None = None,
) -> None:
    """Write a SCHEMA message to stdout.

    Args:
        stream_name: The stream name.
        schema: The JSON schema.
        key_properties: The key properties.
        bookmark_properties: The bookmark properties.
        stream_alias: An alias to use in place of the stream name.
    """
    write_message(
        SchemaMessage(
            stream=(stream_alias or stream_name),
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
