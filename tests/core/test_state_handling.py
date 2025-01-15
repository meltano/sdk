"""Test catalog selection features."""

from __future__ import annotations

import logging
import uuid

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
        },
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
        },
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
        },
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


def test_irresumable_state():
    stream_state = {}
    latest_record = {"updated_at": "2021-05-17T20:41:16Z"}
    replication_key = "updated_at"
    is_sorted = False
    check_sorted = False

    _state.increment_state(
        stream_state,
        latest_record=latest_record,
        replication_key=replication_key,
        is_sorted=is_sorted,
        check_sorted=check_sorted,
    )

    assert stream_state == {
        "progress_markers": {
            "Note": "Progress is not resumable if interrupted.",
            "replication_key": "updated_at",
            "replication_key_value": "2021-05-17T20:41:16Z",
        },
    }


def test_null_replication_value(caplog):
    stream_state = {
        "replication_key": "updated_at",
        "replication_key_value": "2021-05-17T20:41:16Z",
    }
    latest_record = {"updated_at": None}
    replication_key = "updated_at"
    is_sorted = True
    check_sorted = False

    with caplog.at_level(logging.WARNING):
        _state.increment_state(
            stream_state,
            latest_record=latest_record,
            replication_key=replication_key,
            is_sorted=is_sorted,
            check_sorted=check_sorted,
        )

    assert stream_state["replication_key_value"] == "2021-05-17T20:41:16Z", (
        "State should not be updated."
    )
    assert caplog.records[0].levelname == "WARNING"
    assert "is null" in caplog.records[0].message


def test_uuidv7_replication_value():
    stream_state = {
        "replication_key": "id",
        "replication_key_value": "01931c63-b14e-7ff3-8621-e577ed392dc8",
    }
    new_string_val = "01931c63-b14e-7ff3-8621-e578edbca9a3"

    _state.increment_state(
        stream_state,
        latest_record={"id": uuid.UUID(new_string_val)},
        replication_key="id",
        is_sorted=True,
        check_sorted=True,
    )

    assert stream_state["replication_key_value"] == new_string_val
