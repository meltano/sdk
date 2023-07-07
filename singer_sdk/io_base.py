"""Abstract base classes for all Singer messages IO operations."""

from __future__ import annotations

import abc
import decimal
import json
import logging
import sys
import typing as t
from collections import Counter, defaultdict

from singer_sdk._singerlib import SingerMessageType
from singer_sdk.helpers._compat import final

logger = logging.getLogger(__name__)


class SingerReader(metaclass=abc.ABCMeta):
    """Interface for all plugins reading Singer messages from stdin."""

    @final
    def listen(self, file_input: t.IO[str] | None = None) -> None:
        """Read from input until all messages are processed.

        Args:
            file_input: Readable stream of messages. Defaults to standard in.

        This method is internal to the SDK and should not need to be overridden.
        """
        if not file_input:
            file_input = sys.stdin

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
            json.decoder.JSONDecodeError: raised if any lines are not valid json
        """
        try:
            return json.loads(  # type: ignore[no-any-return]
                line,
                parse_float=decimal.Decimal,
            )
        except json.decoder.JSONDecodeError as exc:
            logger.error("Unable to parse:\n%s", line, exc_info=exc)
            raise

    def _process_lines(self, file_input: t.IO[str]) -> t.Counter[str]:
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
