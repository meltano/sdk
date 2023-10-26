"""Singer message types and utilities."""

from __future__ import annotations

import enum
import sys
import typing as t
from dataclasses import asdict, dataclass, field
from datetime import timezone

import simplejson as json
from dateutil.parser import parse

if t.TYPE_CHECKING:
    from datetime import datetime


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

    type: SingerMessageType = field(init=False)  # noqa: A003
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
            time_extracted=parse(time_extracted) if time_extracted else None,
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

    key_properties: list[str] | None = None
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


def format_message(message: Message) -> str:
    """Format a message as a JSON string.

    Args:
        message: The message to format.

    Returns:
        The formatted message.
    """
    return json.dumps(message.to_dict(), use_decimal=True, default=str)


def write_message(message: Message) -> None:
    """Write a message to stdout.

    Args:
        message: The message to write.
    """
    sys.stdout.write(format_message(message) + "\n")
    sys.stdout.flush()
