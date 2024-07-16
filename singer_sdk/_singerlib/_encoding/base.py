"""Abstract base classes for all Singer messages IO operations."""

from __future__ import annotations

import abc
import enum
import logging
import sys
import typing as t
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone

from singer_sdk._singerlib import exceptions

if sys.version_info < (3, 11):
    from backports.datetime_fromisoformat import MonkeyPatch

    MonkeyPatch.patch_fromisoformat()

logger = logging.getLogger(__name__)


# TODO: Use to default to 'str' here
# https://peps.python.org/pep-0696/
T = t.TypeVar("T", str, bytes)


class SingerMessageType(str, enum.Enum):
    """Singer specification message types."""

    RECORD = "RECORD"
    SCHEMA = "SCHEMA"
    STATE = "STATE"
    ACTIVATE_VERSION = "ACTIVATE_VERSION"
    BATCH = "BATCH"


def exclude_null_dict(pairs: list[tuple[str, t.Any]]) -> dict[str, t.Any]:
    """Exclude null values from a dictionary.

    Args:
        pairs: The dictionary key-value pairs.

    Returns:
        The filtered key-value pairs.
    """
    return {key: value for key, value in pairs if value is not None}


@dataclass
class Message:
    """Singer base message."""

    type: SingerMessageType = field(init=False)
    """The message type."""

    def to_dict(self) -> dict[str, t.Any]:
        """Return a dictionary representation of the message.

        Returns:
            A dictionary with the defined message fields.
        """
        return asdict(self, dict_factory=exclude_null_dict)

    @classmethod
    def from_dict(
        cls: t.Type[Message],  # noqa: UP006
        data: dict[str, t.Any],
    ) -> Message:
        """Create an encoding from a dictionary.

        Args:
            data: The dictionary to create the message from.

        Returns:
            The created message.
        """
        data.pop("type")
        return cls(**data)


@dataclass
class RecordMessage(Message):
    """Singer record message."""

    stream: str
    """The stream name."""

    record: dict[str, t.Any]
    """The record data."""

    version: int | None = None
    """The record version."""

    time_extracted: datetime | None = None
    """The time the record was extracted."""

    @classmethod
    def from_dict(cls: type[RecordMessage], data: dict[str, t.Any]) -> RecordMessage:
        """Create a record message from a dictionary.

        This overrides the default conversion logic, since it uses unnecessary
        deep copying and is very slow.

        Args:
            data: The dictionary to create the message from.

        Returns:
            The created message.
        """
        time_extracted = data.get("time_extracted")
        return cls(
            stream=data["stream"],
            record=data["record"],
            version=data.get("version"),
            time_extracted=datetime.fromisoformat(time_extracted)
            if time_extracted
            else None,
        )

    def to_dict(self) -> dict[str, t.Any]:
        """Return a dictionary representation of the message.

        This overrides the default conversion logic, since it uses unnecessary
        deep copying and is very slow.

        Returns:
            A dictionary with the defined message fields.
        """
        result: dict[str, t.Any] = {
            "type": "RECORD",
            "stream": self.stream,
            "record": self.record,
        }
        if self.version is not None:
            result["version"] = self.version
        if self.time_extracted is not None:
            result["time_extracted"] = self.time_extracted
        return result

    def __post_init__(self) -> None:
        """Post-init processing.

        Raises:
            ValueError: If the time_extracted is not timezone-aware.
        """
        self.type = SingerMessageType.RECORD
        if self.time_extracted and not self.time_extracted.tzinfo:
            msg = (
                "'time_extracted' must be either None or an aware datetime (with a "
                "time zone)"
            )
            raise ValueError(msg)

        if self.time_extracted:
            self.time_extracted = self.time_extracted.astimezone(timezone.utc)


@dataclass
class SchemaMessage(Message):
    """Singer schema message."""

    stream: str
    """The stream name."""

    schema: dict[str, t.Any]
    """The schema definition."""

    key_properties: t.Sequence[str] | None = None
    """The key properties."""

    bookmark_properties: list[str] | None = None
    """The bookmark properties."""

    def __post_init__(self) -> None:
        """Post-init processing.

        Raises:
            ValueError: If bookmark_properties is not a string or list of strings.
        """
        self.type = SingerMessageType.SCHEMA

        if isinstance(self.bookmark_properties, (str, bytes)):
            self.bookmark_properties = [self.bookmark_properties]
        if self.bookmark_properties and not isinstance(self.bookmark_properties, list):
            msg = "bookmark_properties must be a string or list of strings"
            raise ValueError(msg)


@dataclass
class StateMessage(Message):
    """Singer state message."""

    value: dict[str, t.Any]
    """The state value."""

    def __post_init__(self) -> None:
        """Post-init processing."""
        self.type = SingerMessageType.STATE


@dataclass
class ActivateVersionMessage(Message):
    """Singer activate version message."""

    stream: str
    """The stream name."""

    version: int
    """The version to activate."""

    def __post_init__(self) -> None:
        """Post-init processing."""
        self.type = SingerMessageType.ACTIVATE_VERSION


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


class GenericSingerWriter(t.Generic[T], metaclass=abc.ABCMeta):
    """Interface for all plugins writing Singer messages as strings or bytes."""

    def format_message(self, message: Message) -> T:
        """Format a message as a JSON string.

        Args:
            message: The message to format.

        Returns:
            The formatted message.
        """
        return self.serialize_json(message.to_dict())

    @abc.abstractmethod
    def serialize_json(self, obj: object) -> T: ...

    @abc.abstractmethod
    def write_message(self, message: Message) -> None: ...
