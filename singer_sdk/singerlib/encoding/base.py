"""Abstract base classes for all Singer messages IO operations."""

from __future__ import annotations

import abc
import enum
import logging
import sys
import typing as t
from collections import Counter, defaultdict

from singer_sdk.singerlib import exceptions

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

    def __init__(self) -> None:
        """Initialize the reader."""
        super().__init__()
        self._current_message: T | None = None

    def process_lines(
        self,
        file_input: t.IO[T] | None,
        callbacks: dict[str, t.Callable[[dict], None]],
    ) -> t.Counter[str]:
        """Internal method to process jsonl lines from a Singer tap.

        Args:
            file_input: Readable stream of messages, each on a separate line.
            callbacks: Dictionary of message type to callback function.

        Returns:
            A counter object for the processed lines.
        """
        stats: dict[str, int] = defaultdict(int)
        filein = file_input or self.default_input
        for line in filein:
            self._current_message = line

            line_dict = self.deserialize_json(line)
            self.assert_line_requires(line_dict, requires={"type"})

            record_type: SingerMessageType = line_dict["type"]
            if callback := callbacks.get(record_type):
                callback(line_dict)
            else:
                self._process_unknown_message(line_dict)

            stats[record_type] += 1

        return Counter(**stats)

    @property
    @abc.abstractmethod
    def default_input(self) -> t.IO[T]:
        """Default input stream for the reader."""

    @staticmethod
    def assert_line_requires(line_dict: dict, requires: set[str]) -> None:
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
    def deserialize_json(self, line: T) -> dict:
        """Deserialize a line of json."""

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
    def serialize_message(self, message: M) -> T:
        """Serialize a message into a line of json."""

    @abc.abstractmethod
    def write_message(self, message: M) -> None:
        """Write a message to stdout."""
