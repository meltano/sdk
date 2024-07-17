from __future__ import annotations

import datetime
import decimal
import logging
import sys
import typing as t

import msgspec

from singer_sdk._singerlib.exceptions import InvalidInputLine

from ._base import GenericSingerReader, GenericSingerWriter

logger = logging.getLogger(__name__)


class Message(msgspec.Struct, tag_field="type", tag=str.upper):
    """Singer base message."""

    def to_dict(self):  # noqa: ANN202
        return {f: getattr(self, f) for f in self.__struct_fields__}


class RecordMessage(Message, tag="RECORD"):
    """Singer RECORD message."""

    stream: str
    record: t.Dict[str, t.Any]  # noqa: UP006
    version: t.Union[int, None] = None  # noqa: UP007
    time_extracted: t.Union[datetime.datetime, None] = None  # noqa: UP007

    def __post_init__(self) -> None:
        """Post-init processing.

        Raises:
            ValueError: If the time_extracted is not timezone-aware.
        """
        if self.time_extracted and not self.time_extracted.tzinfo:
            msg = (
                "'time_extracted' must be either None or an aware datetime (with a "
                "time zone)"
            )
            raise ValueError(msg)

        if self.time_extracted:
            self.time_extracted = self.time_extracted.astimezone(datetime.timezone.utc)


class SchemaMessage(Message, tag="SCHEMA"):
    """Singer SCHEMA message."""

    stream: str
    schema: t.Dict[str, t.Any]  # noqa: UP006
    key_properties: t.List[str]  # noqa: UP006
    bookmark_properties: t.Union[t.List[str], None] = None  # noqa: UP006, UP007

    def __post_init__(self) -> None:
        """Post-init processing.

        Raises:
            ValueError: If bookmark_properties is not a string or list of strings.
        """
        if isinstance(self.bookmark_properties, (str, bytes)):
            self.bookmark_properties = [self.bookmark_properties]
        if self.bookmark_properties and not isinstance(self.bookmark_properties, list):
            msg = "bookmark_properties must be a string or list of strings"
            raise ValueError(msg)


class StateMessage(Message, tag="STATE"):
    """Singer state message."""

    value: t.Dict[str, t.Any]  # noqa: UP006
    """The state value."""


class ActivateVersionMessage(Message, tag="ACTIVATE_VERSION"):
    """Singer activate version message."""

    stream: str
    """The stream name."""

    version: int
    """The version to activate."""


def enc_hook(obj: t.Any) -> t.Any:  # noqa: ANN401
    """Encoding type helper for non native types.

    Args:
        obj: the item to be encoded

    Returns:
        The object converted to the appropriate type, default is str
    """
    return obj.isoformat(sep="T") if isinstance(obj, datetime.datetime) else str(obj)


def dec_hook(type: type, obj: t.Any) -> t.Any:  # noqa: ARG001, A002, ANN401
    """Decoding type helper for non native types.

    Args:
        type: the type given
        obj: the item to be decoded

    Returns:
        The object converted to the appropriate type, default is str.
    """
    return str(obj)


encoder = msgspec.json.Encoder(enc_hook=enc_hook, decimal_format="number")
decoder = msgspec.json.Decoder(
    t.Union[
        RecordMessage,
        SchemaMessage,
        StateMessage,
        ActivateVersionMessage,
    ],
    dec_hook=dec_hook,
    float_hook=decimal.Decimal,
)


class MsgSpecReader(GenericSingerReader[str]):
    """Base class for all plugins reading Singer messages as strings from stdin."""

    default_input = sys.stdin

    def deserialize_json(self, line: str) -> dict:  # noqa: PLR6301
        """Deserialize a line of json.

        Args:
            line: A single line of json.

        Returns:
            A dictionary of the deserialized json.

        Raises:
            InvalidInputLine: If the line cannot be parsed
        """
        try:
            return decoder.decode(line).to_dict()  # type: ignore[no-any-return]
        except msgspec.DecodeError as exc:
            logger.exception("Unable to parse:\n%s", line)
            msg = f"Unable to parse line as JSON: {line}"
            raise InvalidInputLine(msg) from exc


class MsgSpecWriter(GenericSingerWriter[bytes, Message]):
    """Interface for all plugins writing Singer messages to stdout."""

    def serialize_message(self, message: Message) -> bytes:  # noqa: PLR6301
        """Serialize a dictionary into a line of json.

        Args:
            message: A Singer message object.

        Returns:
            A string of serialized json.
        """
        return encoder.encode(message)

    def write_message(self, message: Message) -> None:
        """Write a message to stdout.

        Args:
            message: The message to write.
        """
        sys.stdout.buffer.write(self.format_message(message) + b"\n")
        sys.stdout.flush()
