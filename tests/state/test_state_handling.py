from datetime import datetime

import pytest

from singer_sdk._state import (
    _create_in_partitions_list,
    finalize_state_progress_markers,
    get_starting_replication_value,
    get_state_if_exists,
    get_state_partitions_list,
    get_writeable_state,
    increment_state,
    reset_state_progress_markers,
    write_replication_key_signpost,
    write_starting_replication_value,
)
from singer_sdk._state.schema import PartitionState, StreamState, TapState
from singer_sdk.exceptions import InvalidStreamSortException


@pytest.fixture
def dirty_state() -> TapState[str]:
    return TapState[str].from_dict(
        {
            "bookmarks": {
                "Issues": {
                    "partitions": [
                        {
                            "context": {"org": "aaronsteers", "repo": "target-athena"},
                            "progress_markers": {
                                "Note": "This is not resumable until finalized.",
                                "replication_key": "updated_at",
                                "replication_key_value": "2021-05-17T20:41:16Z",
                            },
                        },
                        {
                            "context": {
                                "org": "andrewcstewart",
                                "repo": "target-athena",
                            },
                            "progress_markers": {
                                "Note": "This is not resumable until finalized.",
                                "replication_key": "updated_at",
                                "replication_key_value": "2021-05-11T18:07:11Z",
                            },
                        },
                        {"context": {"org": "VirusEnabled", "repo": "Athena"}},
                    ],
                    "progress_markers": {
                        "Note": "This is not resumable until finalized.",
                        "replication_key": "updated_at",
                        "replication_key_value": "2021-05-11T18:07:11Z",
                    },
                },
            }
        }
    )


@pytest.fixture
def cleared_state() -> TapState[str]:
    return TapState[str].from_dict(
        {
            "bookmarks": {
                "Issues": {
                    "partitions": [
                        {"context": {"org": "aaronsteers", "repo": "target-athena"}},
                        {"context": {"org": "andrewcstewart", "repo": "target-athena"}},
                        {"context": {"org": "VirusEnabled", "repo": "Athena"}},
                    ],
                },
            }
        }
    )


@pytest.fixture
def finalized_state() -> TapState[str]:
    return TapState[str].from_dict(
        {
            "bookmarks": {
                "Issues": {
                    "partitions": [
                        {
                            "context": {"org": "aaronsteers", "repo": "target-athena"},
                            "replication_key": "updated_at",
                            "replication_key_value": "2021-05-17T20:41:16Z",
                        },
                        {
                            "context": {
                                "org": "andrewcstewart",
                                "repo": "target-athena",
                            },
                            "replication_key": "updated_at",
                            "replication_key_value": "2021-05-11T18:07:11Z",
                        },
                        {"context": {"org": "VirusEnabled", "repo": "Athena"}},
                    ],
                    "replication_key": "updated_at",
                    "replication_key_value": "2021-05-11T18:07:11Z",
                },
            }
        }
    )


@pytest.fixture
def finalized_state_with_signpost() -> TapState[str]:
    return TapState[str].from_dict(
        {
            "bookmarks": {
                "Issues": {
                    "partitions": [
                        {
                            "context": {"org": "aaronsteers", "repo": "target-athena"},
                            "replication_key": "updated_at",
                            "replication_key_value": "2021-05-17T20:41:16Z",
                        },
                        {
                            "context": {
                                "org": "andrewcstewart",
                                "repo": "target-athena",
                            },
                            "replication_key": "updated_at",
                            "replication_key_value": "2021-05-11T18:07:11Z",
                        },
                        {"context": {"org": "VirusEnabled", "repo": "Athena"}},
                    ],
                    "replication_key": "updated_at",
                    "replication_key_value": "2020-05-11T18:07:11Z",
                },
            }
        }
    )


def test_get_state(finalized_state: TapState[str]):
    """Test getting state for a stream or partition."""
    assert get_state_if_exists(TapState(), "Issues") is None
    assert (
        get_state_if_exists(
            TapState(bookmarks={"Issues": StreamState()}),
            "Issues",
            {"org": "MeltanoLabs", "repo": "target-athena"},
        )
        is None
    )

    with pytest.raises(ValueError, match="contains duplicate entries for partition"):
        get_state_if_exists(
            TapState(
                bookmarks={
                    "Issues": StreamState(
                        partitions=[
                            PartitionState(
                                context={"org": "MeltanoLabs", "repo": "target-athena"}
                            ),
                            PartitionState(
                                context={"org": "MeltanoLabs", "repo": "target-athena"}
                            ),
                        ]
                    )
                }
            ),
            "Issues",
            {"org": "MeltanoLabs", "repo": "target-athena"},
        )

    state = finalized_state
    stream_state = get_state_if_exists(state, "Issues")

    assert stream_state is not None
    assert isinstance(stream_state, StreamState)
    assert stream_state.replication_key == "updated_at"
    assert stream_state.replication_key_value == "2021-05-11T18:07:11Z"

    partition_state = get_state_if_exists(
        state,
        "Issues",
        {
            "org": "aaronsteers",
            "repo": "target-athena",
        },
    )
    assert partition_state is not None
    assert isinstance(partition_state, PartitionState)
    assert partition_state.replication_key == "updated_at"
    assert partition_state.replication_key_value == "2021-05-17T20:41:16Z"

    partition_state = get_state_if_exists(
        state,
        "Issues",
        {
            "org": "missing",
            "repo": "target-athena",
        },
    )
    assert partition_state is None


def test_get_partitions(finalized_state: TapState[str]):
    """Test getting state partitions for a stream."""
    state = finalized_state
    partitions = get_state_partitions_list(state, "Issues")

    assert partitions is not None
    assert len(partitions) == 3
    assert get_state_partitions_list(state, "Missing") is None


def test_create_partition(finalized_state: TapState[str]):
    """Test creating state partitions for a stream."""
    state = finalized_state
    stream_state = get_state_if_exists(state, "Issues")

    assert isinstance(stream_state, StreamState)

    context = {"org": "MeltanoLabs", "repo": "target-athena"}
    new_partition = _create_in_partitions_list(
        stream_state.partitions,
        context,
    )

    assert new_partition.context == context
    assert new_partition.replication_key is None
    assert new_partition.replication_key_value is None
    assert len(stream_state.partitions) == 4


def test_get_writable_state(finalized_state: TapState[str]):
    """Test retrieving a writeable state object for a stream or partition."""
    with pytest.raises(ValueError):
        get_writeable_state(None, "null_state")

    state = finalized_state
    stream_state = get_writeable_state(state, "new_stream")

    assert isinstance(stream_state, StreamState)
    assert stream_state.replication_key is None
    assert stream_state.replication_key_value is None

    partition_state = get_writeable_state(
        state,
        "Issues",
        {"org": "aaronsteers", "repo": "target-athena"},
    )

    assert isinstance(partition_state, PartitionState)
    assert partition_state.context == {"org": "aaronsteers", "repo": "target-athena"}
    assert partition_state.replication_key == "updated_at"
    assert partition_state.replication_key_value == "2021-05-17T20:41:16Z"

    partition_state = get_writeable_state(
        state,
        "other_stream",
        {"region": "west", "state": "WA"},
    )

    assert isinstance(partition_state, PartitionState)
    assert partition_state.context == {"region": "west", "state": "WA"}
    assert partition_state.replication_key is None
    assert partition_state.replication_key_value is None


@pytest.mark.parametrize(
    "input,output",
    [
        (datetime(2022, 1, 1), "2022-01-01T00:00:00+00:00"),
        (10, 10),
        ("2022-01-01T00:00:00+00:00", "2022-01-01T00:00:00+00:00"),
    ],
    ids=["datetime", "integer", "date-string"],
)
def test_write_signpost(finalized_state: TapState[str], input, output):
    """Test writing a signpost value to the state object."""
    state = finalized_state
    stream_state = state.bookmarks["Issues"]
    write_replication_key_signpost(stream_state, input)

    assert stream_state.replication_key_signpost == output


@pytest.mark.parametrize(
    "input,output",
    [
        (datetime(2022, 1, 1), "2022-01-01T00:00:00+00:00"),
        (10, 10),
        ("2022-01-01T00:00:00+00:00", "2022-01-01T00:00:00+00:00"),
    ],
    ids=["datetime", "integer", "date-string"],
)
def test_write_starting_value(finalized_state: TapState[str], input, output):
    """Test writing an initial value to the state object."""
    state = finalized_state
    stream_state = state.bookmarks["Issues"]
    write_starting_replication_value(stream_state, input)

    assert get_starting_replication_value(None) is None
    assert get_starting_replication_value(stream_state) == output


def test_state_increment_unsorted_markers(dirty_state: TapState[str]):
    """Test incrementing an unsorted stream with progress_markers."""
    stream_state = dirty_state.bookmarks["Issues"]
    increment_state(
        stream_state,
        {"updated_at": "2022-01-01"},
        "updated_at",
        False,
    )
    assert stream_state.replication_key_value is None
    assert stream_state.progress_markers is not None
    assert stream_state.progress_markers.replication_key == "updated_at"
    assert stream_state.progress_markers.replication_key_value == "2022-01-01"


def test_state_increment_unsorted_fresh(finalized_state: TapState[str]):
    """Test incrementing a fresh unsorted stream."""
    stream_state = finalized_state.bookmarks["Issues"]
    increment_state(
        stream_state,
        {"updated_at": "2022-01-01"},
        "updated_at",
        False,
    )
    assert stream_state.progress_markers is not None
    assert stream_state.progress_markers.replication_key == "updated_at"
    assert stream_state.progress_markers.replication_key_value == "2022-01-01"
    assert stream_state.progress_markers.Note is not None


def test_state_increment_sorted(dirty_state: TapState[str]):
    """Test incrementing state in main stream or partition state body."""
    stream_state = dirty_state.bookmarks["Issues"]
    increment_state(
        stream_state,
        {"updated_at": "2022-01-01"},
        "updated_at",
        True,
    )
    assert stream_state.replication_key == "updated_at"
    assert stream_state.replication_key_value == "2022-01-01"


def test_state_increment_sort_error(finalized_state: TapState[str]):
    """Test incrementing state in when new bookmark value is not ordered."""
    stream_state = finalized_state.bookmarks["Issues"]
    with pytest.raises(InvalidStreamSortException):
        increment_state(
            stream_state,
            {"updated_at": "2021-01-01"},
            "updated_at",
            True,
        )


def test_state_reset(dirty_state: TapState[str], cleared_state: TapState[str]):
    """Test state reset."""
    state = dirty_state
    for stream_state in state.bookmarks.values():
        reset_state_progress_markers(stream_state)
        for partition_state in stream_state.partitions:
            reset_state_progress_markers(partition_state)
    assert state == cleared_state


def test_state_finalize(dirty_state: TapState[str], finalized_state: TapState[str]):
    """Test state finalization."""
    state = dirty_state
    for stream_state in state.bookmarks.values():
        finalize_state_progress_markers(stream_state)
        for partition_state in stream_state.partitions:
            finalize_state_progress_markers(partition_state)
    assert state == finalized_state


def test_state_finalize_unsorted_signpost(
    dirty_state: TapState[str],
    finalized_state_with_signpost: TapState[str],
):
    """Test state finalization when a signpost is selected as new replication value."""
    state = dirty_state
    for stream_state in state.bookmarks.values():
        write_replication_key_signpost(stream_state, "2020-05-11T18:07:11Z")
        finalize_state_progress_markers(stream_state)
        for partition_state in stream_state.partitions:
            finalize_state_progress_markers(partition_state)
    assert state == finalized_state_with_signpost
