from __future__ import annotations

import io
import json
import typing as t
from contextlib import redirect_stdout

import pytest

from singer_sdk import Stream, Tap
from singer_sdk.io_base import SingerMessageType


class Parent(Stream):
    """A parent stream."""

    name = "parent"
    schema: t.ClassVar[dict] = {
        "type": "object",
        "properties": {
            "id": {"type": "integer"},
        },
    }

    def get_child_context(
        self,
        record: dict,
        context: dict | None,  # noqa: ARG002
    ) -> dict | None:
        """Create context for children streams."""
        return {
            "pid": record["id"],
        }

    def get_records(self, context: dict | None):  # noqa: ARG002
        """Get dummy records."""
        yield {"id": 1}
        yield {"id": 2}
        yield {"id": 3}


class Child(Stream):
    """A child stream."""

    name = "child"
    schema: t.ClassVar[dict] = {
        "type": "object",
        "properties": {
            "id": {"type": "integer"},
            "pid": {"type": "integer"},
        },
    }
    parent_stream_type = Parent

    def get_records(self, context: dict | None):  # noqa: ARG002
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


def _get_messages(tap: Tap):
    """Redirect stdout to a buffer."""
    buf = io.StringIO()
    with redirect_stdout(buf):
        tap.sync_all()
    buf.seek(0)
    lines = buf.read().splitlines()
    return [json.loads(line) for line in lines]


def test_parent_context_fields_in_child(tap: MyTap):
    """Test that parent context fields are available in child streams."""
    parent_stream = tap.streams["parent"]
    child_stream = tap.streams["child"]
    messages = _get_messages(tap)

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

    # Child records are emitted
    child_record_messages = messages[2:5]
    assert child_record_messages
    assert all(msg["type"] == SingerMessageType.RECORD for msg in child_record_messages)
    assert all(msg["stream"] == child_stream.name for msg in child_record_messages)
    assert all("pid" in msg["record"] for msg in child_record_messages)


def test_skip_deleted_parent_child_streams(
    tap: MyTap,
    caplog: pytest.LogCaptureFixture,
):
    """Test tap output with parent stream deselected."""
    parent_stream = tap.streams["parent"]

    buf = io.StringIO()
    with redirect_stdout(buf), caplog.at_level("WARNING"):
        parent_stream._sync_children(None)

    buf.seek(0)

    assert not buf.read().splitlines()
    assert len(caplog.records) == 1
    assert caplog.records[0].levelname == "WARNING"
    assert caplog.records[0].message == (
        "Context for child streams of 'parent' is null, "
        "skipping sync of any child streams"
    )


def test_child_deselected_parent(tap_with_deselected_parent: MyTap):
    """Test tap output with parent stream deselected."""
    parent_stream = tap_with_deselected_parent.streams["parent"]
    child_stream = tap_with_deselected_parent.streams["child"]

    assert not parent_stream.selected
    assert parent_stream.has_selected_descendents

    messages = _get_messages(tap_with_deselected_parent)

    # First message is a schema for the child stream, not the parent
    assert messages[0]
    assert messages[0]["type"] == SingerMessageType.SCHEMA
    assert messages[0]["stream"] == child_stream.name
    assert messages[0]["schema"] == child_stream.schema

    # Child records are emitted
    child_record_messages = messages[1:4]
    assert child_record_messages
    assert all(msg["type"] == SingerMessageType.RECORD for msg in child_record_messages)
    assert all(msg["stream"] == child_stream.name for msg in child_record_messages)
    assert all("pid" in msg["record"] for msg in child_record_messages)


def test_one_parent_many_children(tap: MyTap):
    """Test tap output with parent stream deselected."""

    class ParentMany(Stream):
        """A parent stream."""

        name = "parent_many"
        schema: t.ClassVar[dict] = {
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "children": {"type": "array", "items": {"type": "integer"}},
            },
        }

        def get_records(
            self,
            context: dict | None,  # noqa: ARG002
        ) -> t.Iterable[dict | tuple[dict, dict | None]]:
            yield {"id": "1", "children": [1, 2, 3]}

        def generate_child_contexts(
            self,
            record: dict,
            context: dict | None,  # noqa: ARG002
        ) -> t.Iterable[dict | None]:
            for child_id in record["children"]:
                yield {"child_id": child_id, "pid": record["id"]}

    class ChildMany(Stream):
        """A child stream."""

        name = "child_many"
        schema: t.ClassVar[dict] = {
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "pid": {"type": "integer"},
            },
        }
        parent_stream_type = ParentMany

        def get_records(self, context: dict | None):
            """Get dummy records."""
            yield {
                "id": context["child_id"],
                "composite_id": f"{context['pid']}-{context['child_id']}",
            }

    class MyTapMany(Tap):
        """A tap with streams having a parent-child relationship."""

        name = "my-tap-many"

        def discover_streams(self):
            """Discover streams."""
            return [
                ParentMany(self),
                ChildMany(self),
            ]

    tap = MyTapMany()
    parent_stream = tap.streams["parent_many"]
    child_stream = tap.streams["child_many"]

    messages = _get_messages(tap)

    # Parent schema is emitted
    assert messages[0]
    assert messages[0]["type"] == SingerMessageType.SCHEMA
    assert messages[0]["stream"] == parent_stream.name
    assert messages[0]["schema"] == parent_stream.schema

    # Child schemas are emitted
    schema_messages = messages[1:8:3]
    assert schema_messages
    assert all(msg["type"] == SingerMessageType.SCHEMA for msg in schema_messages)
    assert all(msg["stream"] == child_stream.name for msg in schema_messages)
    assert all(msg["schema"] == child_stream.schema for msg in schema_messages)

    # Child records are emitted
    child_record_messages = messages[2:9:3]
    assert child_record_messages
    assert all(msg["type"] == SingerMessageType.RECORD for msg in child_record_messages)
    assert all(msg["stream"] == child_stream.name for msg in child_record_messages)
    assert all("pid" in msg["record"] for msg in child_record_messages)

    # State messages are emitted
    state_messages = messages[3:10:3]
    assert state_messages
    assert all(msg["type"] == SingerMessageType.STATE for msg in state_messages)

    # Parent record is emitted
    assert messages[10]
    assert messages[10]["type"] == SingerMessageType.RECORD
