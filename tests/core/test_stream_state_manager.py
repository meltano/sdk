"""Tests for StreamStateManager class."""

from __future__ import annotations

import typing as t

import pytest

from singer_sdk.singerlib.catalog import REPLICATION_FULL_TABLE, REPLICATION_INCREMENTAL
from singer_sdk.streams._state import StreamStateManager

if t.TYPE_CHECKING:
    from singer_sdk.helpers import types


@pytest.fixture
def tap_state() -> types.TapState:
    """Return an empty tap state dict."""
    return {}


@pytest.fixture
def state_manager(tap_state: types.TapState) -> StreamStateManager:
    """Return a StreamStateManager instance."""
    return StreamStateManager(
        tap_name="test-tap",
        stream_name="test_stream",
        tap_state=tap_state,
    )


class TestStreamStateManagerInit:
    """Tests for StreamStateManager initialization."""

    def test_init_sets_attributes(self, tap_state: types.TapState):
        """Test that init sets all attributes correctly."""
        manager = StreamStateManager(
            tap_name="my-tap",
            stream_name="my_stream",
            tap_state=tap_state,
            state_partitioning_keys=["tenant_id"],
        )
        assert manager.tap_name == "my-tap"
        assert manager.stream_name == "my_stream"
        assert manager.tap_state is tap_state
        assert manager.state_partitioning_keys == ["tenant_id"]
        assert manager.is_flushed is True

    def test_init_default_partitioning_keys(self, tap_state: types.TapState):
        """Test that state_partitioning_keys defaults to None."""
        manager = StreamStateManager(
            tap_name="test-tap",
            stream_name="test_stream",
            tap_state=tap_state,
        )
        assert manager.state_partitioning_keys is None


class TestStreamState:
    """Tests for stream_state property."""

    def test_stream_state_creates_bookmark(
        self,
        state_manager: StreamStateManager,
        tap_state: types.TapState,
    ):
        """Test that accessing stream_state creates bookmark entry."""
        assert tap_state == {}
        _ = state_manager.stream_state
        assert tap_state == {"bookmarks": {"test_stream": {}}}

    def test_stream_state_returns_existing(self, tap_state: types.TapState):
        """Test that stream_state returns existing bookmark."""
        tap_state["bookmarks"] = {
            "test_stream": {"replication_key_value": "2021-01-01"}
        }
        manager = StreamStateManager(
            tap_name="test-tap",
            stream_name="test_stream",
            tap_state=tap_state,
        )
        state = manager.stream_state
        assert state == {"replication_key_value": "2021-01-01"}


class TestGetContextState:
    """Tests for get_context_state method."""

    def test_no_context_returns_stream_state(
        self,
        state_manager: StreamStateManager,
        tap_state: types.TapState,
    ):
        """Test that None context returns stream state."""
        state = state_manager.get_context_state(None)
        assert state == {}
        assert tap_state == {"bookmarks": {"test_stream": {}}}

    def test_context_creates_partition(
        self,
        state_manager: StreamStateManager,
        tap_state: types.TapState,
    ):
        """Test that context creates partition state."""
        context = {"tenant_id": "abc123"}
        state = state_manager.get_context_state(context)
        assert state == {"context": {"tenant_id": "abc123"}}
        assert tap_state["bookmarks"]["test_stream"]["partitions"] == [
            {"context": {"tenant_id": "abc123"}}
        ]

    def test_context_returns_existing_partition(self, tap_state: types.TapState):
        """Test that existing partition is returned."""
        tap_state["bookmarks"] = {
            "test_stream": {
                "partitions": [
                    {
                        "context": {"tenant_id": "abc123"},
                        "replication_key_value": "2021-01-01",
                    }
                ]
            }
        }
        manager = StreamStateManager(
            tap_name="test-tap",
            stream_name="test_stream",
            tap_state=tap_state,
        )
        context = {"tenant_id": "abc123"}
        state = manager.get_context_state(context)
        assert state["replication_key_value"] == "2021-01-01"


class TestGetStatePartitionContext:
    """Tests for get_state_partition_context method."""

    def test_none_context_returns_none(self, state_manager: StreamStateManager):
        """Test that None context returns None."""
        result = state_manager.get_state_partition_context(None)
        assert result is None

    def test_no_partitioning_keys_returns_full_context(
        self,
        state_manager: StreamStateManager,
    ):
        """Test that full context is returned when no partitioning keys."""
        context = {"tenant_id": "abc", "date": "2021-01-01"}
        result = state_manager.get_state_partition_context(context)
        assert result == context

    def test_partitioning_keys_filters_context(self, tap_state: types.TapState):
        """Test that context is filtered by partitioning keys."""
        manager = StreamStateManager(
            tap_name="test-tap",
            stream_name="test_stream",
            tap_state=tap_state,
            state_partitioning_keys=["tenant_id"],
        )
        context = {"tenant_id": "abc", "date": "2021-01-01", "extra": "ignored"}
        result = manager.get_state_partition_context(context)
        assert result == {"tenant_id": "abc"}


class TestIsFlushed:
    """Tests for is_flushed flag behavior."""

    def test_initial_state_is_flushed(self, state_manager: StreamStateManager):
        """Test that is_flushed starts as True."""
        assert state_manager.is_flushed is True

    def test_finalize_state_marks_not_flushed(self, state_manager: StreamStateManager):
        """Test that finalize_state sets is_flushed to False."""
        state_manager.finalize_state({})
        assert state_manager.is_flushed is False


class TestIncrementState:
    """Tests for increment_state method."""

    def test_increment_state_updates_bookmark(
        self,
        state_manager: StreamStateManager,
        tap_state: types.TapState,
    ):
        """Test that increment_state updates replication key value."""
        state_manager.increment_state(
            {"updated_at": "2021-05-17T20:41:16Z"},
            replication_key="updated_at",
            is_sorted=True,
            check_sorted=False,
        )
        stream_state = tap_state["bookmarks"]["test_stream"]
        assert stream_state["replication_key"] == "updated_at"
        assert stream_state["replication_key_value"] == "2021-05-17T20:41:16Z"

    def test_increment_state_with_context(self, tap_state: types.TapState):
        """Test increment_state with partition context."""
        manager = StreamStateManager(
            tap_name="test-tap",
            stream_name="test_stream",
            tap_state=tap_state,
        )
        context = {"tenant_id": "abc123"}
        manager.increment_state(
            {"updated_at": "2021-05-17T20:41:16Z"},
            context=context,
            replication_key="updated_at",
            is_sorted=True,
            check_sorted=False,
        )
        partitions = tap_state["bookmarks"]["test_stream"]["partitions"]
        assert len(partitions) == 1
        assert partitions[0]["context"] == context
        assert partitions[0]["replication_key_value"] == "2021-05-17T20:41:16Z"

    def test_increment_unsorted_creates_progress_markers(
        self,
        state_manager: StreamStateManager,
        tap_state: types.TapState,
    ):
        """Test that unsorted streams create progress markers."""
        state_manager.increment_state(
            {"updated_at": "2021-05-17T20:41:16Z"},
            replication_key="updated_at",
            is_sorted=False,
            check_sorted=False,
        )
        stream_state = tap_state["bookmarks"]["test_stream"]
        assert "progress_markers" in stream_state
        assert stream_state["progress_markers"]["replication_key"] == "updated_at"


class TestFinalizeState:
    """Tests for finalize_state method."""

    def test_finalize_promotes_progress_markers(self, tap_state: types.TapState):
        """Test that finalize_state promotes progress markers."""
        tap_state["bookmarks"] = {
            "test_stream": {
                "progress_markers": {
                    "replication_key": "updated_at",
                    "replication_key_value": "2021-05-17T20:41:16Z",
                }
            }
        }
        manager = StreamStateManager(
            tap_name="test-tap",
            stream_name="test_stream",
            tap_state=tap_state,
        )
        state = manager.stream_state
        manager.finalize_state(state)

        assert "progress_markers" not in state
        assert state["replication_key"] == "updated_at"
        assert state["replication_key_value"] == "2021-05-17T20:41:16Z"


class TestFinalizeProgressMarkers:
    """Tests for finalize_progress_markers method."""

    def test_finalize_with_no_state_uses_stream_state(
        self,
        state_manager: StreamStateManager,
        tap_state: types.TapState,
    ):
        """Test finalize_progress_markers with no state creates stream bookmark."""
        state_manager.finalize_progress_markers()
        assert "bookmarks" in tap_state
        assert "test_stream" in tap_state["bookmarks"]
        assert state_manager.is_flushed is False

    def test_finalize_with_partitions(self, tap_state: types.TapState):
        """Test finalize_progress_markers with partitions."""
        manager = StreamStateManager(
            tap_name="test-tap",
            stream_name="test_stream",
            tap_state=tap_state,
        )
        partitions = [{"tenant_id": "a"}, {"tenant_id": "b"}]
        manager.finalize_progress_markers(partitions=partitions)

        stream_state = tap_state["bookmarks"]["test_stream"]
        assert "partitions" in stream_state
        assert len(stream_state["partitions"]) == 2

    def test_finalize_non_empty_state(self):
        """Test that finalize_progress_markers does nothing with empty state."""
        tap_state: types.TapState = {
            "bookmarks": {
                "test_stream": {
                    "replication_key": "updated_at",
                    "replication_key_value": "2021-05-17T20:41:16Z",
                }
            }
        }
        manager = StreamStateManager(
            tap_name="test-tap",
            stream_name="test_stream",
            tap_state=tap_state,
        )
        manager.finalize_progress_markers(
            state={
                "replication_key": "updated_at",
                "replication_key_value": "2021-05-17T20:41:16Z",
                "progress_markers": {
                    "replication_key": "updated_at",
                    "replication_key_value": "2021-05-17T20:41:16Z",
                },
                "starting_replication_value": "2021-05-17T20:41:16Z",
                "replication_key_signpost": "2021-05-17T20:41:16Z",
            }
        )
        assert tap_state == {
            "bookmarks": {
                "test_stream": {
                    "replication_key": "updated_at",
                    "replication_key_value": "2021-05-17T20:41:16Z",
                }
            }
        }


class TestResetProgressMarkers:
    """Tests for reset_progress_markers method."""

    def test_reset_clears_progress_markers(self, tap_state: types.TapState):
        """Test that reset_progress_markers clears markers."""
        tap_state["bookmarks"] = {
            "test_stream": {
                "progress_markers": {
                    "replication_key": "updated_at",
                    "replication_key_value": "2021-05-17T20:41:16Z",
                }
            }
        }
        manager = StreamStateManager(
            tap_name="test-tap",
            stream_name="test_stream",
            tap_state=tap_state,
        )
        state = manager.stream_state
        manager.reset_progress_markers(state)

        assert "progress_markers" not in state

    def test_reset_clears_progress_markers_with_partitions(
        self, tap_state: types.TapState
    ):
        """Test that reset_progress_markers clears markers with partitions."""
        tap_state["bookmarks"] = {
            "test_stream": {
                "partitions": [
                    {
                        "context": {"tenant_id": "a"},
                        "progress_markers": {
                            "replication_key": "updated_at",
                            "replication_key_value": "2021-05-17T20:41:16Z",
                        },
                    },
                ],
            }
        }
        manager = StreamStateManager(
            tap_name="test-tap",
            stream_name="test_stream",
            tap_state=tap_state,
        )
        state = manager.stream_state

        # Verify progress markers exist in partition before reset
        assert "partitions" in state
        assert len(state["partitions"]) == 1
        assert "progress_markers" in state["partitions"][0]

        manager.reset_progress_markers(
            state,
            partitions=[{"tenant_id": "a"}],
        )

        # Verify progress markers are cleared from partition
        assert "progress_markers" not in state
        assert "partitions" in state
        assert len(state["partitions"]) == 1
        assert "progress_markers" not in state["partitions"][0]
        assert state["partitions"][0] == {"context": {"tenant_id": "a"}}

    def test_reset_all_partitions_when_state_is_none(self, tap_state: types.TapState):
        """Test that reset_progress_markers resets all partitions when state=None."""
        tap_state["bookmarks"] = {
            "test_stream": {
                "partitions": [
                    {
                        "context": {"tenant_id": "a"},
                        "progress_markers": {
                            "replication_key": "updated_at",
                            "replication_key_value": "2021-05-17T20:41:16Z",
                        },
                    },
                    {
                        "context": {"tenant_id": "b"},
                        "progress_markers": {
                            "replication_key": "updated_at",
                            "replication_key_value": "2021-05-18T10:30:00Z",
                        },
                    },
                ],
            }
        }
        manager = StreamStateManager(
            tap_name="test-tap",
            stream_name="test_stream",
            tap_state=tap_state,
        )

        # Verify progress markers exist before reset
        state = manager.stream_state
        assert len(state["partitions"]) == 2
        assert "progress_markers" in state["partitions"][0]
        assert "progress_markers" in state["partitions"][1]

        # Reset with state=None to reset all partitions
        manager.reset_progress_markers(
            state=None,
            partitions=[{"tenant_id": "a"}, {"tenant_id": "b"}],
        )

        # Verify all partition progress markers are cleared
        assert len(state["partitions"]) == 2
        assert "progress_markers" not in state["partitions"][0]
        assert "progress_markers" not in state["partitions"][1]
        assert state["partitions"][0] == {"context": {"tenant_id": "a"}}
        assert state["partitions"][1] == {"context": {"tenant_id": "b"}}


class TestGetStatePartitions:
    """Tests for get_state_partitions method."""

    def test_no_partitions_returns_none(self, state_manager: StreamStateManager):
        """Test that None is returned when no partitions exist."""
        result = state_manager.get_state_partitions()
        assert result is None

    def test_returns_existing_partitions(self, tap_state: types.TapState):
        """Test that existing partitions are returned."""
        tap_state["bookmarks"] = {
            "test_stream": {
                "partitions": [
                    {"context": {"tenant_id": "a"}},
                    {"context": {"tenant_id": "b"}},
                ]
            }
        }
        manager = StreamStateManager(
            tap_name="test-tap",
            stream_name="test_stream",
            tap_state=tap_state,
        )
        partitions = manager.get_state_partitions()
        assert partitions is not None
        assert len(partitions) == 2


class TestGetStartingReplicationValue:
    """Tests for get_starting_replication_value method."""

    def test_full_table_returns_none(self, tap_state: types.TapState):
        """Test that FULL_TABLE replication returns None."""
        tap_state["bookmarks"] = {
            "test_stream": {"replication_key_value": "2021-01-01"}
        }
        manager = StreamStateManager(
            tap_name="test-tap",
            stream_name="test_stream",
            tap_state=tap_state,
        )
        result = manager.get_starting_replication_value(None, REPLICATION_FULL_TABLE)
        assert result is None

    def test_incremental_returns_value(self, tap_state: types.TapState):
        """Test that INCREMENTAL replication returns bookmark value."""
        tap_state["bookmarks"] = {
            "test_stream": {
                "replication_key": "updated_at",
                "starting_replication_value": "2021-05-17T20:41:16Z",
            }
        }
        manager = StreamStateManager(
            tap_name="test-tap",
            stream_name="test_stream",
            tap_state=tap_state,
        )
        result = manager.get_starting_replication_value(None, REPLICATION_INCREMENTAL)
        assert result == "2021-05-17T20:41:16Z"

    def test_no_state_returns_none(self, state_manager: StreamStateManager):
        """Test that no prior state returns None."""
        result = state_manager.get_starting_replication_value(
            None, REPLICATION_INCREMENTAL
        )
        assert result is None


class TestIsStateNonResumable:
    """Tests for is_state_non_resumable method."""

    def test_no_progress_markers_is_resumable(self, state_manager: StreamStateManager):
        """Test that state without progress markers is resumable."""
        assert state_manager.is_state_non_resumable() is False

    def test_with_progress_markers_is_non_resumable(self, tap_state: types.TapState):
        """Test that state with progress markers is non-resumable."""
        tap_state["bookmarks"] = {
            "test_stream": {
                "progress_markers": {
                    "replication_key": "updated_at",
                    "replication_key_value": "2021-05-17T20:41:16Z",
                }
            }
        }
        manager = StreamStateManager(
            tap_name="test-tap",
            stream_name="test_stream",
            tap_state=tap_state,
        )
        assert manager.is_state_non_resumable() is True


class TestWriteStartingReplicationValue:
    """Tests for write_starting_replication_value method."""

    def test_full_table_does_nothing(
        self,
        state_manager: StreamStateManager,
        tap_state: types.TapState,
    ):
        """Test that FULL_TABLE replication does not write starting value."""
        state_manager.write_starting_replication_value(
            context=None,
            replication_method=REPLICATION_FULL_TABLE,
            replication_key="updated_at",
            config={},
        )
        # Stream state should exist but be empty (no starting_replication_value)
        assert tap_state == {}

    def test_incremental_writes_starting_value(self, tap_state: types.TapState):
        """Test that INCREMENTAL replication writes starting value from state."""
        tap_state["bookmarks"] = {
            "test_stream": {
                "replication_key": "updated_at",
                "replication_key_value": "2021-05-17T20:41:16Z",
            }
        }
        manager = StreamStateManager(
            tap_name="test-tap",
            stream_name="test_stream",
            tap_state=tap_state,
        )
        manager.write_starting_replication_value(
            context=None,
            replication_method=REPLICATION_INCREMENTAL,
            replication_key="updated_at",
            config={},
        )
        state = manager.stream_state
        assert state["starting_replication_value"] == "2021-05-17T20:41:16Z"

    def test_uses_start_date_when_no_state(self, state_manager: StreamStateManager):
        """Test that start_date from config is used when no prior state."""
        state_manager.write_starting_replication_value(
            context=None,
            replication_method=REPLICATION_INCREMENTAL,
            replication_key="updated_at",
            config={"start_date": "2020-01-01"},
        )
        state = state_manager.stream_state
        assert state["starting_replication_value"] == "2020-01-01"

    def test_compare_start_date_fn_used(self, tap_state: types.TapState):
        """Test that compare_start_date_fn is called to compare values."""
        tap_state["bookmarks"] = {
            "test_stream": {
                "replication_key": "updated_at",
                "replication_key_value": "2021-05-17",
            }
        }
        manager = StreamStateManager(
            tap_name="test-tap",
            stream_name="test_stream",
            tap_state=tap_state,
        )

        # This function always returns the second argument
        def compare_fn(a: str, b: str) -> str:  # noqa: ARG001
            return b

        manager.write_starting_replication_value(
            context=None,
            replication_method=REPLICATION_INCREMENTAL,
            replication_key="updated_at",
            config={"start_date": "2020-01-01"},
            compare_start_date_fn=compare_fn,
        )
        state = manager.stream_state
        # compare_fn returns second arg (start_date)
        assert state["starting_replication_value"] == "2020-01-01"

    def test_compare_start_date_no_fn_used(self, tap_state: types.TapState):
        """Test that compare_start_date_fn is not called if no function is provided."""
        tap_state["bookmarks"] = {
            "test_stream": {
                "replication_key": "updated_at",
                "replication_key_value": "2021-05-17",
            }
        }
        manager = StreamStateManager(
            tap_name="test-tap",
            stream_name="test_stream",
            tap_state=tap_state,
        )

        manager.write_starting_replication_value(
            context=None,
            replication_method=REPLICATION_INCREMENTAL,
            replication_key="updated_at",
            config={"start_date": "2020-01-01"},
            compare_start_date_fn=None,
        )
        state = manager.stream_state
        assert state["starting_replication_value"] == "2021-05-17"

    def test_no_replication_key_does_nothing(self, state_manager: StreamStateManager):
        """Test that no replication key does not write starting value."""
        state_manager.write_starting_replication_value(
            context=None,
            replication_method=REPLICATION_INCREMENTAL,
            replication_key=None,
            config={},
        )
        assert state_manager.stream_state == {"starting_replication_value": None}


class TestWriteReplicationKeySignpost:
    """Tests for write_replication_key_signpost method."""

    def test_writes_signpost_value(
        self,
        state_manager: StreamStateManager,
        tap_state: types.TapState,
    ):
        """Test that signpost value is written to state."""
        state_manager.write_replication_key_signpost(None, "2021-05-17T20:41:16Z")
        state = tap_state["bookmarks"]["test_stream"]
        assert state["replication_key_signpost"] == "2021-05-17T20:41:16Z"

    def test_empty_value_does_nothing(
        self,
        state_manager: StreamStateManager,
        tap_state: dict,
    ):
        """Test that empty value does not write signpost."""
        state_manager.write_replication_key_signpost(None, "")
        assert tap_state == {}

    def test_none_value_does_nothing(
        self,
        state_manager: StreamStateManager,
        tap_state: dict,
    ):
        """Test that None value does not write signpost."""
        state_manager.write_replication_key_signpost(None, None)
        assert tap_state == {}


class TestLogSortError:
    """Tests for log_sort_error method."""

    def test_logs_error_message(
        self,
        state_manager: StreamStateManager,
        caplog: pytest.LogCaptureFixture,
    ):
        """Test that sort error is logged."""
        ex = Exception("Sort order violated")
        state_manager.log_sort_error(
            ex=ex,
            current_context=None,
            record_count=10,
            partition_record_count=10,
        )
        assert len(caplog.records) == 1
        assert "Sorting error" in caplog.records[0].message
        assert "test_stream" in caplog.records[0].message
        assert "record #10" in caplog.records[0].message

    def test_logs_partition_info_when_different(
        self,
        state_manager: StreamStateManager,
        caplog: pytest.LogCaptureFixture,
    ):
        """Test that partition record count is included when different."""
        ex = Exception("Sort order violated")
        state_manager.log_sort_error(
            ex=ex,
            current_context={"tenant_id": "abc"},
            record_count=100,
            partition_record_count=5,
        )
        assert "partition record #5" in caplog.records[0].message
        assert "tenant_id" in caplog.records[0].message
