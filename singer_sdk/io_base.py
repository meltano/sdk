"""Abstract base classes for all Singer messages IO operations."""

from __future__ import annotations

import abc
import decimal
import logging
import sys
import typing as t
from collections import Counter, defaultdict
from datetime import datetime  # noqa: TCH003

import msgspec

from singer_sdk._singerlib import SingerMessageType
from singer_sdk.helpers._compat import final

logger = logging.getLogger(__name__)


class MessageType(msgspec.Struct):
    """Struct used to obtain type from SingerMessages for routing."""

    type: t.Optional[str] = None  # noqa: A003 UP007


class Record(msgspec.Struct):
    """Struct for decoding Singer RECORD Messages."""

    type: str  # noqa: A003
    stream: str
    record: msgspec.Raw
    version: t.Optional[int] = None  # noqa: UP007
    time_extracted: t.Optional[datetime] = None  # noqa: UP007


decoder = msgspec.json.Decoder(dec_hook=str)
type_decoder = msgspec.json.Decoder(type=MessageType)


class SingerReader(metaclass=abc.ABCMeta):
    """Interface for all plugins reading Singer messages from stdin."""

    stream_structs: dict[str, msgspec.Struct] = {}  # noqa: RUF012

    @final
    def listen(self, file_input: t.IO[str] | None = None) -> None:
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
            raise Exception(msg)

    def deserialize_json(self, line: str) -> dict:
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

    def deserialize_record(self, line: str) -> dict:
        """Deserialize a line of json.

        Args:
            line: A single line of json.

        Returns:
            A dictionary of the deserialized json.

        Raises:
            msgspec.DecodeError: raised if any lines are not valid json
        """
        try:
            line_struct = msgspec.json.decode(line, type=Record)
        except msgspec.DecodeError as exc:
            logger.error("Unable to parse:\n%s", line, exc_info=exc)
            raise

        line_dict = msgspec.structs.asdict(line_struct)

        try:
            record = msgspec.json.decode(
                line_struct.record,
                type=self.stream_structs[line_struct.stream],
            )
        except msgspec.DecodeError as exc:
            logger.error("Unable to parse:\n%s", line, exc_info=exc)
            raise

        line_dict.update({"record": msgspec.structs.asdict(record)})

        return line_dict

    def register_stream_struct(self, schema_msg: dict[str, t.Any]) -> None:
        """Register stream struct converted from schema message.

        Args:
            schema_msg: A single line of json containing a SCHMEA message.
        """
        stream: str = schema_msg.get("stream")
        properties: dict = schema_msg.get("schema", {}).get("properties")
        fields: list = []
        for field_name, definition in properties.items():
            field_json_type = definition.get("type", [])[0]
            is_nullable: bool = "null" in definition.get("type", [])
            field_type = None
            if field_json_type == "string":
                field_type = str
            elif field_json_type == "integer":
                field_type = int
            elif field_json_type == "number":
                field_type = decimal.Decimal

            if field_type is not None and is_nullable:
                field_type = t.Optional[field_type]

            if field_type is None:
                fields.append(field_name)
            else:
                fields.append((field_name, field_type))

        self.stream_structs[stream] = msgspec.defstruct(stream, fields=fields)

    def _process_lines(self, file_input: t.IO[str]) -> t.Counter[str]:
        """Internal method to process jsonl lines from a Singer tap.

        Args:
            file_input: Readable stream of messages, each on a separate line.

        Returns:
            A counter object for the processed lines.
        """
        stats: dict[str, int] = defaultdict(int)
        for line in file_input:
            header: MessageType = type_decoder.decode(line)
            if header.type == "RECORD":
                line_dict = self.deserialize_record(line)
            else:
                line_dict = self.deserialize_json(line)

            self._assert_line_requires(line_dict, requires={"type"})

            record_type: SingerMessageType = line_dict["type"]
            if record_type == SingerMessageType.SCHEMA:
                self._process_schema_message(line_dict)
                self.register_stream_struct(line_dict)

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
