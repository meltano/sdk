"""Abstract base classes for all Singer messages IO operations."""

from __future__ import annotations

import abc
import decimal
import logging
import sys
import typing as t
from collections import Counter, defaultdict

import msgspec

from singer_sdk._singerlib.messages import Message, SingerMessageType
from singer_sdk.helpers._compat import final

logger = logging.getLogger(__name__)

decoder = msgspec.json.Decoder(dec_hook=str, float_hook=decimal.Decimal)
encoder = msgspec.json.Encoder(enc_hook=str, decimal_format="number")
msg_buffer = bytearray(64)


class SingerReader(metaclass=abc.ABCMeta):
    """Interface for all plugins reading Singer messages from stdin."""

    stream_structs: dict[str, msgspec.Struct] = {}  # noqa: RUF012

    @final
    def listen(self, file_input: t.BinaryIO | None = None) -> None:
        """Read from input until all messages are processed.

        Args:
            file_input: Readable stream of messages. Defaults to standard in.

        This method is internal to the SDK and should not need to be overridden.
        """
        if not file_input:
            file_input = sys.stdin.buffer

        self._process_lines(file_input)
        self._process_endofpipe()

    @staticmethod
    def _assert_line_requires(line_dict: dict, requires: set[str]) -> None:
        """Check if dictionary .

        Args:
            line_dict: TODO
            requires: TODO

        Raises:
            Exception: TODO
        """
        if not requires.issubset(line_dict):
            missing = requires - set(line_dict)
            msg = f"Line is missing required {', '.join(missing)} key(s): {line_dict}"
            raise Exception(msg)  # TODO: Raise a more specific exception

    def deserialize_json(self, line: bytes) -> dict:
        """Deserialize a line of json.

        Args:
            line: A single line of json.

        Returns:
            A dictionary of the deserialized json.

        Raises:
            msgspec.DecodeError: raised if any lines are not valid json
        """
        try:
            return decoder.decode(  # type: ignore[no-any-return]
                line,
            )
        except msgspec.DecodeError as exc:
            logger.error("Unable to parse:\n%s", line, exc_info=exc)
            raise

    def _process_lines(self, file_input: t.BinaryIO) -> t.Counter[str]:
        """Internal method to process jsonl lines from a Singer tap.

        Args:
            file_input: Readable stream of messages, each on a separate line.

        Returns:
            A counter object for the processed lines.
        """
        stats: dict[str, int] = defaultdict(int)
        for line in file_input:
            line_dict = self.deserialize_json(line)
            self._assert_line_requires(line_dict, requires={"type"})

            record_type: SingerMessageType = line_dict["type"]
            if record_type == SingerMessageType.SCHEMA:
                self._process_schema_message(line_dict)

            elif record_type == SingerMessageType.RECORD:
                self._process_record_message(line_dict)

            elif record_type == SingerMessageType.ACTIVATE_VERSION:
                self._process_activate_version_message(line_dict)

            elif record_type == SingerMessageType.STATE:
                self._process_state_message(line_dict)

            elif record_type == SingerMessageType.BATCH:
                self._process_batch_message(line_dict)

            else:
                self._process_unknown_message(line_dict)

            stats[record_type] += 1

        return Counter(**stats)

    @abc.abstractmethod
    def _process_schema_message(self, message_dict: dict) -> None:
        ...

    @abc.abstractmethod
    def _process_record_message(self, message_dict: dict) -> None:
        ...

    @abc.abstractmethod
    def _process_state_message(self, message_dict: dict) -> None:
        ...

    @abc.abstractmethod
    def _process_activate_version_message(self, message_dict: dict) -> None:
        ...

    @abc.abstractmethod
    def _process_batch_message(self, message_dict: dict) -> None:
        ...

    def _process_unknown_message(self, message_dict: dict) -> None:
        """Internal method to process unknown message types from a Singer tap.

        Args:
            message_dict: Dictionary representation of the Singer message.

        Raises:
            ValueError: raised if a message type is not recognized
        """
        record_type = message_dict["type"]
        msg = f"Unknown message type '{record_type}' in message."
        raise ValueError(msg)

    def _process_endofpipe(self) -> None:
        logger.debug("End of pipe reached")


class SingerWriter:
    """Interface for all plugins writting Singer messages to stdout."""

    def format_message(self, message: Message) -> bytes:
        """Format a message as a JSON string.

        Args:
            message: The message to format.

        Returns:
            The formatted message.
        """
        encoder.encode_into(message.to_dict(), msg_buffer)
        msg_buffer.extend(b"\n")
        return msg_buffer

    def write_message(self, message: Message) -> None:
        """Write a message to stdout.

        Args:
            message: The message to write.
        """
        sys.stdout.buffer.write(self.format_message(message))
        sys.stdout.flush()
