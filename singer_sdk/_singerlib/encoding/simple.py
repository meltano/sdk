from __future__ import annotations

import sys
import typing as t

from singer_sdk._singerlib.json import deserialize_json, serialize_json

from .base import GenericSingerReader, GenericSingerWriter

if t.TYPE_CHECKING:
    from singer_sdk._singerlib.messages import Message


class SingerReader(GenericSingerReader[str]):
    """Base class for all plugins reading Singer messages as strings from stdin."""

    default_input = sys.stdin

    def deserialize_json(self, line: str) -> dict:  # noqa: PLR6301
        """Deserialize a line of json.

        Args:
            line: A single line of json.

        Returns:
            A dictionary of the deserialized json.
        """
        return deserialize_json(line)


class SingerWriter(GenericSingerWriter[str]):
    """Interface for all plugins writing Singer messages to stdout."""

    def serialize_json(self, obj: object) -> str:  # noqa: PLR6301
        """Serialize a dictionary into a line of json.

        Args:
            obj: A Python object usually a dict.

        Returns:
            A string of serialized json.
        """
        return serialize_json(obj)

    def write_message(self, message: Message) -> None:
        """Write a message to stdout.

        Args:
            message: The message to write.
        """
        sys.stdout.write(self.format_message(message) + "\n")
        sys.stdout.flush()
