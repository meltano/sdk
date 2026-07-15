"""BATCH message support."""

from __future__ import annotations

import typing as t
from dataclasses import dataclass, field

from singer.messages import Message
from singer.messages import SingerMessageType as _SingerMessageType

__all__ = [
    "BaseBatchFileEncoding",
    "BatchMessage",
]


@dataclass(slots=True)
class BaseBatchFileEncoding:
    """Base class for batch file encodings."""

    # Base encoding fields
    format: str
    """The format of the batch file (e.g. ``jsonl``, ``parquet``, ``arrow``)."""

    compression: str | None = None
    """The compression of the batch file."""

    @classmethod
    def from_dict(cls, data: dict[str, t.Any]) -> BaseBatchFileEncoding:
        """Create an encoding from a dictionary.

        Args:
            data: The dictionary to create the encoding from.

        Returns:
            The created encoding.
        """
        return cls(**data)


@dataclass(slots=True)
class BatchMessage(Message):
    """Singer BATCH message."""

    stream: str
    """The stream name."""

    encoding: BaseBatchFileEncoding
    """The file encoding of the batch."""

    manifest: list[str] = field(default_factory=list)
    """The manifest of files in the batch."""

    version: int | None = None
    """If syncing in FULL_TABLE mode, the start time as an epoch timestamp int."""

    def __post_init__(self) -> None:
        """Post-init processing."""
        if isinstance(self.encoding, dict):  # type: ignore[unreachable]
            self.encoding = BaseBatchFileEncoding.from_dict(self.encoding)  # type: ignore[unreachable]

        self.type = _SingerMessageType.BATCH
