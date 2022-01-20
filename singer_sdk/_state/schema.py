"""State artifact schema."""

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Dict, Generic, List, Optional, TypeVar

from mashumaro import DataClassJSONMixin

TReplKey = TypeVar("TReplKey", int, str, datetime, date, float)


@dataclass
class Bookmark(Generic[TReplKey], DataClassJSONMixin):
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

    context: Optional[Dict[str, Any]] = None


@dataclass
class StreamState(BookmarkWithMarkers[TReplKey]):
    """Bookmark for a partitioned stream."""

    partitions: List[PartitionState[TReplKey]] = field(default_factory=list)


@dataclass
class TapState(Generic[TReplKey], DataClassJSONMixin):
    """Singer tap state."""

    bookmarks: Dict[str, StreamState[TReplKey]] = field(default_factory=dict)
