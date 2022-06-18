from __future__ import annotations

import io
import json
from contextlib import redirect_stdout

import pytest

from singer_sdk import Stream, Tap
from singer_sdk.io_base import SingerMessageType


class Parent(Stream):
    """A parent stream."""

    name = "parent"
    schema = {
        "type": "object",
        "properties": {
            "id": {"type": "integer"},
        },
    }

    def get_child_context(self, record: dict, context: dict | None) -> dict:
        """Create context for children streams."""
        return {
            "pid": record["id"],
        }

    def get_records(self, context: dict | None):
        """Get dummy records."""
        yield {"id": 1}
        yield {"id": 2}
        yield {"id": 3}


class Child(Stream):
    """A child stream."""

    name = "child"
    schema = {
        "type": "object",
        "properties": {
            "id": {"type": "integer"},
            "pid": {"type": "integer"},
        },
    }
    parent_stream_type = Parent

    def get_records(self, context: dict | None):
        """Get dummy records."""
        yield {"id": 1}
        yield {"id": 2}
        yield {"id": 3}


class MyTap(Tap):
    """A tap with streams having a parent-child relationship."""

    name = "my-tap"

    def discover_streams(self):
        """Discover streams."""
        return [
            Parent(self),
            Child(self),
        ]


@pytest.fixture
def tap():
    """A tap with streams having a parent-child relationship."""
    return MyTap()


@pytest.fixture
def tap_with_deselected_parent(tap: MyTap):
    """A tap with a parent stream deselected."""
    original = tap.catalog["parent"].metadata[()].selected
    tap.catalog["parent"].metadata[()].selected = False
    yield tap
    tap.catalog["parent"].metadata[()].selected = original


def test_parent_context_fields_in_child(tap: MyTap):
    """Test that parent context fields are available in child streams."""
    parent_stream = tap.streams["parent"]
    child_stream = tap.streams["child"]

    with io.BytesIO() as buf, io.TextIOWrapper(buf) as to, redirect_stdout(to):
        tap.sync_all()

        to.seek(0)
        lines = to.read().splitlines()
        messages = [json.loads(line) for line in lines]

    # Parent schema is emitted
    assert messages[0]
    assert messages[0]["type"] == SingerMessageType.SCHEMA
    assert messages[0]["stream"] == parent_stream.name
    assert messages[0]["schema"] == parent_stream.schema

    # Child schema is emitted
    assert messages[1]
    assert messages[1]["type"] == SingerMessageType.SCHEMA
    assert messages[1]["stream"] == child_stream.name
    assert messages[1]["schema"] == child_stream.schema

    # Child records are emitted, skip state message in between
    child_record_messages = messages[2], *messages[4:6]
    assert child_record_messages
    assert all(msg["type"] == SingerMessageType.RECORD for msg in child_record_messages)
    assert all(msg["stream"] == child_stream.name for msg in child_record_messages)
    assert all("pid" in msg["record"] for msg in child_record_messages)


def test_child_deselected_parent(tap_with_deselected_parent: MyTap):
    """Test tap output with parent stream deselected."""
    parent_stream = tap_with_deselected_parent.streams["parent"]
    child_stream = tap_with_deselected_parent.streams["child"]

    assert not parent_stream.selected
    assert parent_stream.has_selected_descendents

    with io.BytesIO() as buf, io.TextIOWrapper(buf) as to, redirect_stdout(to):
        tap_with_deselected_parent.sync_all()

        buf.seek(0)
        lines = buf.read().splitlines()
        messages = [json.loads(line) for line in lines]

    # First message is a schema for the child stream, not the parent
    assert messages[0]
    assert messages[0]["type"] == SingerMessageType.SCHEMA
    assert messages[0]["stream"] == child_stream.name
    assert messages[0]["schema"] == child_stream.schema

    # Child records are emitted, skip state message in between
    child_record_messages = messages[1], *messages[3:5]
    assert child_record_messages
    assert all(msg["type"] == SingerMessageType.RECORD for msg in child_record_messages)
    assert all(msg["stream"] == child_stream.name for msg in child_record_messages)
    assert all("pid" in msg["record"] for msg in child_record_messages)
