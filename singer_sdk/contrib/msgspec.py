"""Singer message encoding using msgspec."""

from __future__ import annotations

import datetime
import decimal
import logging
import sys
import typing as t

import msgspec

from singer_sdk.singerlib.encoding.base import GenericSingerReader, GenericSingerWriter
from singer_sdk.singerlib.encoding.simple import Message
from singer_sdk.singerlib.exceptions import InvalidInputLine

logger = logging.getLogger(__name__)


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
decoder = msgspec.json.Decoder(dec_hook=dec_hook, float_hook=decimal.Decimal)
_jsonl_msg_buffer = bytearray(64)


def serialize_jsonl(obj: object, **kwargs: t.Any) -> bytes:  # noqa: ARG001
    """Serialize a dictionary into a line of jsonl.

    Args:
        obj: A Python object usually a dict.
        **kwargs: Optional key word arguments.

    Returns:
        A bytes of serialized json.
    """
    encoder.encode_into(obj, _jsonl_msg_buffer)
    _jsonl_msg_buffer.extend(b"\n")
    return _jsonl_msg_buffer


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
            return decoder.decode(line)  # type: ignore[no-any-return]
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
        return serialize_jsonl(message.to_dict())

    def write_message(self, message: Message) -> None:
        """Write a message to stdout.

        Args:
            message: The message to write.
        """
        sys.stdout.buffer.write(self.format_message(message))
        sys.stdout.flush()
