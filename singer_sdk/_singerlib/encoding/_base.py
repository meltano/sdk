"""Abstract base classes for all Singer messages IO operations."""

from __future__ import annotations

import abc
import enum
import logging
import sys
import typing as t
from collections import Counter, defaultdict

from singer_sdk._singerlib import exceptions

if sys.version_info < (3, 11):
    from backports.datetime_fromisoformat import MonkeyPatch

    MonkeyPatch.patch_fromisoformat()

logger = logging.getLogger(__name__)


# TODO: Use to default to 'str' here
# https://peps.python.org/pep-0696/
T = t.TypeVar("T", str, bytes)
M = t.TypeVar("M")


class SingerMessageType(str, enum.Enum):
    """Singer specification message types."""

    RECORD = "RECORD"
    SCHEMA = "SCHEMA"
    STATE = "STATE"
    ACTIVATE_VERSION = "ACTIVATE_VERSION"
    BATCH = "BATCH"


class GenericSingerReader(t.Generic[T], metaclass=abc.ABCMeta):
    """Interface for all plugins reading Singer messages as strings or bytes."""

    @t.final
    def listen(self, file_input: t.IO[T] | None = None) -> None:
        """Read from input until all messages are processed.

        Args:
            file_input: Readable stream of messages. Defaults to standard in.
        """
        self._process_lines(file_input or self.default_input)
        self._process_endofpipe()

    def _process_lines(self, file_input: t.IO[T]) -> t.Counter[str]:
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

    @property
    @abc.abstractmethod
    def default_input(self) -> t.IO[T]: ...

    @staticmethod
    def _assert_line_requires(line_dict: dict, requires: set[str]) -> None:
        """Check if dictionary .

        Args:
            line_dict: TODO
            requires: TODO

        Raises:
            InvalidInputLine: raised if any required keys are missing
        """
        if not requires.issubset(line_dict):
            missing = requires - set(line_dict)
            msg = f"Line is missing required {', '.join(missing)} key(s): {line_dict}"
            raise exceptions.InvalidInputLine(msg)

    @abc.abstractmethod
    def deserialize_json(self, line: T) -> dict: ...

    @abc.abstractmethod
    def _process_schema_message(self, message_dict: dict) -> None: ...

    @abc.abstractmethod
    def _process_record_message(self, message_dict: dict) -> None: ...

    @abc.abstractmethod
    def _process_state_message(self, message_dict: dict) -> None: ...

    @abc.abstractmethod
    def _process_activate_version_message(self, message_dict: dict) -> None: ...

    @abc.abstractmethod
    def _process_batch_message(self, message_dict: dict) -> None: ...

    def _process_unknown_message(self, message_dict: dict) -> None:  # noqa: PLR6301
        """Internal method to process unknown message types from a Singer tap.

        Args:
            message_dict: Dictionary representation of the Singer message.

        Raises:
            ValueError: raised if a message type is not recognized
        """
        record_type = message_dict["type"]
        msg = f"Unknown message type '{record_type}' in message."
        raise ValueError(msg)

    def _process_endofpipe(self) -> None:  # noqa: PLR6301
        logger.debug("End of pipe reached")


class GenericSingerWriter(t.Generic[T, M], metaclass=abc.ABCMeta):
    """Interface for all plugins writing Singer messages as strings or bytes."""

    def format_message(self, message: M) -> T:
        """Format a message as a JSON string.

        Args:
            message: The message to format.

        Returns:
            The formatted message.
        """
        return self.serialize_message(message)

    @abc.abstractmethod
    def serialize_message(self, message: M) -> T: ...

    @abc.abstractmethod
    def write_message(self, message: M) -> None: ...
