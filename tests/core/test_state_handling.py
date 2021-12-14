"""Test catalog selection features."""

import pytest

from singer_sdk.helpers import _state


@pytest.fixture
def dirty_state():
    return {
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
                        "context": {"org": "andrewcstewart", "repo": "target-athena"},
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


@pytest.fixture
def cleared_state():
    return {
        "bookmarks": {
            "Issues": {
                "partitions": [
                    {
                        "context": {"org": "aaronsteers", "repo": "target-athena"},
                    },
                    {
                        "context": {"org": "andrewcstewart", "repo": "target-athena"},
                    },
                    {"context": {"org": "VirusEnabled", "repo": "Athena"}},
                ],
            },
        }
    }


@pytest.fixture
def finalized_state():
    return {
        "bookmarks": {
            "Issues": {
                "partitions": [
                    {
                        "context": {"org": "aaronsteers", "repo": "target-athena"},
                        "replication_key": "updated_at",
                        "replication_key_value": "2021-05-17T20:41:16Z",
                    },
                    {
                        "context": {"org": "andrewcstewart", "repo": "target-athena"},
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


def test_state_reset(dirty_state, cleared_state):
    """Test state reset."""
    state = dirty_state
    for stream_state in state["bookmarks"].values():
        _state.reset_state_progress_markers(stream_state)
        for partition_state in stream_state.get("partitions", []):
            _state.reset_state_progress_markers(partition_state)
    assert state == cleared_state


def test_state_finalize(dirty_state, finalized_state):
    """Test state finalization."""
    state = dirty_state
    for stream_state in state["bookmarks"].values():
        _state.finalize_state_progress_markers(stream_state)
        for partition_state in stream_state.get("partitions", []):
            _state.finalize_state_progress_markers(partition_state)
    assert state == finalized_state
