"""Replication configuration objects for Singer streams."""

from __future__ import annotations

import typing as t
from dataclasses import dataclass, field

from singer_sdk.state_comparators import AscendingComparator

if t.TYPE_CHECKING:
    from singer_sdk.state_comparators import StateComparator


@dataclass(frozen=True)
class FullTableReplication:
    """Replication configuration for full-table streams.

    Streams using this configuration fetch all records on every sync run.
    Options like ``replication_key``, ``is_sorted``, and ``check_sorted`` are
    intentionally absent — they have no meaning for full-table replication.

    Example::

        class MyStream(RESTStream):
            replication = FullTableReplication()
    """


@dataclass(frozen=True)
class IncrementalReplication:
    """Replication configuration for incremental streams.

    Args:
        key: Field used as the replication bookmark (e.g. ``"updated_at"``).
        is_sorted: Whether records are guaranteed to arrive sorted by ``key``.
            When ``True``, syncs interrupted mid-stream can resume where they
            left off. Defaults to ``False``.
        check_sorted: Whether to raise
            :exc:`~singer_sdk.exceptions.InvalidStreamSortException` when
            out-of-order records are detected. Defaults to ``True``.
        state_comparator: Comparator controlling bookmark advancement semantics.
            Defaults to :class:`~singer_sdk.AscendingComparator` (``>=``),
            which implements *at-least-once* delivery.

    Example::

        from singer_sdk import RESTStream, StrictAscendingComparator
        from singer_sdk.replication import IncrementalReplication


        class MyStream(RESTStream):
            replication = IncrementalReplication(
                key="updated_at",
                is_sorted=True,
                state_comparator=StrictAscendingComparator(),
            )
    """

    key: str
    is_sorted: bool = False
    check_sorted: bool = True
    state_comparator: StateComparator = field(default_factory=AscendingComparator)


@dataclass(frozen=True)
class LogBasedReplication:
    """Replication configuration for log-based (CDC) streams.

    Example::

        class MyStream(RESTStream):
            replication = LogBasedReplication()
    """


#: Union of all supported replication configuration types.
ReplicationConfig = FullTableReplication | IncrementalReplication | LogBasedReplication

__all__ = [
    "FullTableReplication",
    "IncrementalReplication",
    "LogBasedReplication",
    "ReplicationConfig",
]
