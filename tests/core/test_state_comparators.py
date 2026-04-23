"""Tests for StateComparator classes."""

from __future__ import annotations

import datetime
import typing as t

import pytest

from singer_sdk.state_comparators import (
    AscendingComparator,
    StateComparator,
    StrictAscendingComparator,
)
from singer_sdk.streams._state import StreamStateManager

if t.TYPE_CHECKING:
    from singer_sdk.helpers import types


class TestAscendingComparator:
    @pytest.fixture
    def comparator(self) -> AscendingComparator:
        return AscendingComparator()

    def test_advance_when_greater(self, comparator: AscendingComparator) -> None:
        assert comparator("2024-02-01", "2024-01-01") is True

    def test_advance_when_equal(self, comparator: AscendingComparator) -> None:
        assert comparator("2024-01-01", "2024-01-01") is True

    def test_no_advance_when_less(self, comparator: AscendingComparator) -> None:
        assert comparator("2024-01-01", "2024-02-01") is False

    def test_advance_with_integers(self, comparator: AscendingComparator) -> None:
        assert comparator(42, 41) is True
        assert comparator(41, 41) is True
        assert comparator(40, 41) is False

    def test_preprocess_converts_datetime(
        self,
        comparator: AscendingComparator,
    ) -> None:
        dt = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
        result = comparator.preprocess(dt)
        assert isinstance(result, str)

    def test_advance_with_raw_datetime_vs_stored_string(
        self,
        comparator: AscendingComparator,
    ) -> None:
        """New value from record (datetime) vs stored value (ISO string) should work."""
        dt_new = datetime.datetime(2024, 2, 1, tzinfo=datetime.timezone.utc)
        stored = "2024-01-01T00:00:00+00:00"
        assert comparator(dt_new, stored) is True


class TestStrictAscendingComparator:
    @pytest.fixture
    def comparator(self) -> StrictAscendingComparator:
        return StrictAscendingComparator()

    def test_advance_when_greater(self, comparator: StrictAscendingComparator) -> None:
        assert comparator("2024-02-01", "2024-01-01") is True

    def test_no_advance_when_equal(self, comparator: StrictAscendingComparator) -> None:
        """Key difference from AscendingComparator: equal values do NOT advance."""
        assert comparator("2024-01-01", "2024-01-01") is False

    def test_no_advance_when_less(self, comparator: StrictAscendingComparator) -> None:
        assert comparator("2024-01-01", "2024-02-01") is False

    def test_advance_with_integers(self, comparator: StrictAscendingComparator) -> None:
        assert comparator(42, 41) is True
        assert comparator(41, 41) is False
        assert comparator(40, 41) is False


class TestCustomSubclass:
    def test_custom_comparator_is_callable(self) -> None:
        class MyComparator(StateComparator):
            def is_advance(self, new_value: object, old_value: object) -> bool:
                return new_value != old_value

        c = MyComparator()
        assert c("a", "b") is True
        assert c("a", "a") is False

    def test_custom_preprocess(self) -> None:
        class LowerCaseComparator(StateComparator):
            def preprocess(self, value: object) -> str:
                return str(value).lower()

            def is_advance(self, new_value: object, old_value: object) -> bool:
                return new_value >= old_value  # type: ignore[operator]

        c = LowerCaseComparator()
        assert c("B", "a") is True
        assert c("A", "b") is False


def test_increment_state_ascending_allows_equal_value() -> None:
    """AscendingComparator updates state when new == old (at-least-once)."""
    tap_state: types.TapState = {
        "bookmarks": {
            "test_stream": {"replication_key": "id", "replication_key_value": "5"},
        },
    }
    manager = StreamStateManager(
        stream_name="test_stream",
        tap_state=tap_state,
        is_sorted=True,
        check_sorted=True,
        comparator=AscendingComparator(),
    )
    manager.increment_state({"id": "5"}, replication_key="id")
    assert tap_state["bookmarks"]["test_stream"]["replication_key_value"] == "5"


def test_increment_state_strict_skips_equal_value() -> None:
    """StrictAscendingComparator does NOT update state when new == old."""
    tap_state: types.TapState = {
        "bookmarks": {
            "test_stream": {"replication_key": "id", "replication_key_value": "5"},
        },
    }
    manager = StreamStateManager(
        stream_name="test_stream",
        tap_state=tap_state,
        is_sorted=True,
        check_sorted=False,
        comparator=StrictAscendingComparator(),
    )
    manager.increment_state({"id": "5"}, replication_key="id")
    assert tap_state["bookmarks"]["test_stream"]["replication_key_value"] == "5"


def test_increment_state_strict_advances_when_greater() -> None:
    tap_state: types.TapState = {
        "bookmarks": {
            "test_stream": {"replication_key": "id", "replication_key_value": "5"},
        },
    }
    manager = StreamStateManager(
        stream_name="test_stream",
        tap_state=tap_state,
        is_sorted=True,
        check_sorted=True,
        comparator=StrictAscendingComparator(),
    )
    manager.increment_state({"id": "6"}, replication_key="id")
    assert tap_state["bookmarks"]["test_stream"]["replication_key_value"] == "6"
