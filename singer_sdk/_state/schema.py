"""State artifact schema."""

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Dict, Generic, List, Optional, Type, TypeVar

from mashumaro import DataClassJSONMixin
from mashumaro.config import TO_DICT_ADD_OMIT_NONE_FLAG

TReplKey = TypeVar("TReplKey", int, str, datetime, date, float)


class BaseModel(DataClassJSONMixin):
    """Basic dataclass model."""

    class Config:
        """Dataclass options."""

        code_generation_options = [
            TO_DICT_ADD_OMIT_NONE_FLAG,
        ]


@dataclass
class Bookmark(Generic[TReplKey], BaseModel):
    """Generic and base bookmark for all state, resumable or not."""

    replication_key: Optional[str] = None
    replication_key_value: Optional[TReplKey] = None


@dataclass
class ProgressMarkers(Bookmark[TReplKey]):
    """Progress markers for non-resumable streams."""

    Note: Optional[str] = None


@dataclass
class BookmarkWithMarkers(Bookmark[TReplKey]):
    """Bookmark with support for non-resumable streams."""

    replication_key_signpost: Optional[TReplKey] = None
    starting_replication_value: Optional[TReplKey] = None
    progress_markers: Optional[ProgressMarkers[TReplKey]] = None


@dataclass
class PartitionState(BookmarkWithMarkers[TReplKey]):
    """Bookmark for a stream partition."""

    context: Dict[str, Any] = field(default_factory=dict)


@dataclass
class StreamState(BookmarkWithMarkers[TReplKey]):
    """Bookmark for a partitioned stream."""

    partitions: List[PartitionState[TReplKey]] = field(default_factory=list)

    @classmethod
    def __pre_deserialize__(
        cls: Type["StreamState"],
        d: Dict[Any, Any],
    ) -> Dict[Any, Any]:
        """Process raw dictionary.

        - Add an empty partitions array if none is found.

        Args:
            d: Input dictionary.

        Returns:
            Processed dictionary.
        """
        if d.get("partitions") is None:
            d["partitions"] = []
        return d

    def __post_serialize__(self, d: Dict[Any, Any]) -> Dict[Any, Any]:
        """Process output dictionary.

        - Remove partitions array if it's empty.

        Args:
            d: Output dictionary.

        Returns:
            Processed dictionary.
        """
        if not d["partitions"]:
            d.pop("partitions")
        return d


@dataclass
class TapState(Generic[TReplKey], BaseModel):
    """Singer tap state."""

    bookmarks: Dict[str, StreamState[TReplKey]] = field(default_factory=dict)

    @classmethod
    def __pre_deserialize__(
        cls: Type["TapState"],
        d: Dict[Any, Any],
    ) -> Dict[Any, Any]:
        """Process raw dictionary.

        - Add an empty bookmarks mapping if none is found.
        - Apparently only required in Python 3.6.

        Args:
            d: Input dictionary.

        Returns:
            Processed dictionary.
        """
        if d.get("bookmarks") is None:
            d["bookmarks"] = {}

        return d
