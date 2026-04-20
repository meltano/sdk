"""State comparators for replication key advancement logic."""

from __future__ import annotations

import sys
import typing as t
from abc import ABC, abstractmethod

from singer_sdk.helpers._typing import to_json_compatible

if sys.version_info >= (3, 12):
    from typing import override  # noqa: ICN003
else:
    from typing_extensions import override


class StateComparator(ABC):
    """Abstract base for replication key state comparison.

    Subclass to customize how the SDK determines whether a new replication
    key value represents a state advancement over the previously bookmarked value.

    Two responsibilities are intentionally separated:

    - :meth:`preprocess`: normalize/deserialize raw values before comparison
    - :meth:`is_advance`: define the ordering semantics

    Example::

        class MyComparator(StateComparator):
            def preprocess(self, value):
                return parse_my_date(value)

            def is_advance(self, new_value, old_value):
                return new_value >= old_value
    """

    def preprocess(self, value: t.Any) -> t.Any:  # noqa: ANN401, PLR6301
        """Prepare a raw value for comparison.

        Args:
            value: Raw replication key value from a record or the state.

        Returns:
            A JSON-compatible form of the value, suitable for ordering.
        """
        return to_json_compatible(value)

    @abstractmethod
    def is_advance(self, new_value: t.Any, old_value: t.Any) -> bool:  # noqa: ANN401
        """Return True if new_value represents a state advancement over old_value.

        Both arguments have already been passed through :meth:`preprocess`.

        Args:
            new_value: Preprocessed replication key value from the latest record.
            old_value: Preprocessed replication key value stored in the current state.

        Returns:
            True if the state bookmark should be updated to new_value.
        """
        ...

    def __call__(self, new_value: t.Any, old_value: t.Any) -> bool:  # noqa: ANN401
        """Preprocess then compare two replication key values.

        Args:
            new_value: Raw replication key value from the latest record.
            old_value: Raw replication key value stored in the current state.

        Returns:
            True if the state bookmark should be updated to new_value.
        """
        return self.is_advance(self.preprocess(new_value), self.preprocess(old_value))


class AscendingComparator(StateComparator):
    """Default comparator: advances state when new value >= old value.

    Implements *at-least-once* delivery semantics. Records whose replication key
    equals the current bookmark are re-emitted on the next sync run, so targets
    must be able to handle duplicate rows (upsert / dedup).

    This is the Singer-recommended default because it avoids silently skipping
    records that arrive with a timestamp equal to the bookmark (e.g. multiple
    writes within the same second).
    """

    @override
    def is_advance(self, new_value: t.Any, old_value: t.Any) -> bool:
        """Return True when new_value >= old_value.

        Args:
            new_value: Preprocessed replication key value from the latest record.
            old_value: Preprocessed replication key value stored in the current state.

        Returns:
            True if new_value is greater than or equal to old_value.
        """
        return new_value >= old_value  # type: ignore[no-any-return]


class StrictAscendingComparator(StateComparator):
    """Comparator that advances state only when new value > old value.

    Implements *exactly-once* delivery semantics. Records whose replication key
    equals the current bookmark are **not** re-emitted on the next sync run.

    Use this when:

    - The target cannot tolerate duplicate records (e.g. reverse-ETL destinations).
    - The replication key is guaranteed unique (e.g. an auto-incrementing integer PK).

    .. warning::
        If multiple records share the same replication key value and a sync is
        interrupted mid-batch, those records may be silently skipped on resume.
        Only use this comparator when you are certain ties cannot occur or are
        acceptable to miss.
    """

    @override
    def is_advance(self, new_value: t.Any, old_value: t.Any) -> bool:
        """Return True when new_value > old_value.

        Args:
            new_value: Preprocessed replication key value from the latest record.
            old_value: Preprocessed replication key value stored in the current state.

        Returns:
            True if new_value is strictly greater than old_value.
        """
        return new_value > old_value  # type: ignore[no-any-return]
