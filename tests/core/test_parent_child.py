from __future__ import annotations

import datetime
import io
import logging
import typing as t
from contextlib import redirect_stdout

import pytest
import time_machine

from singer_sdk import Stream, Tap

if t.TYPE_CHECKING:
    from pytest_snapshot.plugin import Snapshot

DATETIME = datetime.datetime(2022, 1, 1, tzinfo=datetime.timezone.utc)


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
@time_machine.travel(DATETIME, tick=False)
def tap():
    """A tap with streams having a parent-child relationship."""
    return MyTap()


@pytest.fixture
def tap_with_deselected_parent(tap: MyTap):
    """A tap with a parent stream deselected."""
    stream_metadata = tap.catalog["parent"].metadata.root
    original = stream_metadata.selected
    stream_metadata.selected = False
    yield tap
    stream_metadata.selected = original


@pytest.fixture
def tap_with_deselected_child(tap: MyTap):
    """A tap with a child stream deselected."""
    stream_metadata = tap.catalog["child"].metadata.root
    original = stream_metadata.selected
    stream_metadata.selected = False
    yield tap
    stream_metadata.selected = original


@time_machine.travel(DATETIME, tick=False)
@pytest.mark.snapshot
def test_parent_context_fields_in_child(
    tap: MyTap,
    caplog: pytest.LogCaptureFixture,
    snapshot: Snapshot,
):
    """Test that parent context fields are available in child streams."""
    buf = io.StringIO()
    with (
        redirect_stdout(buf),
        caplog.at_level("INFO"),
        caplog.filtering(logging.Filter(tap.name)),
    ):
        tap.sync_all()

    buf.seek(0)
    snapshot.assert_match(buf.read(), "singer.jsonl")
    snapshot.assert_match(caplog.text, "stderr.log")


@pytest.mark.snapshot
def test_skip_deleted_parent_child_streams(
    tap: MyTap,
    caplog: pytest.LogCaptureFixture,
    snapshot: Snapshot,
):
    """Test tap output with parent stream deselected."""
    parent_stream = tap.streams["parent"]

    buf = io.StringIO()
    with redirect_stdout(buf), caplog.at_level("WARNING"):
        parent_stream._sync_children(None)

    buf.seek(0)

    assert not buf.read().splitlines()
    snapshot.assert_match(caplog.text, "stderr.log")


@time_machine.travel(DATETIME, tick=False)
@pytest.mark.snapshot
def test_child_deselected_parent(
    tap_with_deselected_parent: MyTap,
    caplog: pytest.LogCaptureFixture,
    snapshot: Snapshot,
):
    """Test tap output with parent stream deselected."""
    parent_stream = tap_with_deselected_parent.streams["parent"]

    assert not parent_stream.selected
    assert parent_stream.has_selected_descendents

    buf = io.StringIO()
    with (
        redirect_stdout(buf),
        caplog.at_level("INFO"),
        caplog.filtering(logging.Filter(tap_with_deselected_parent.name)),
    ):
        tap_with_deselected_parent.sync_all()

    buf.seek(0)

    snapshot.assert_match(buf.read(), "singer.jsonl")
    snapshot.assert_match(caplog.text, "stderr.log")


@time_machine.travel(DATETIME, tick=False)
@pytest.mark.snapshot
def test_deselected_child(
    tap_with_deselected_child: MyTap,
    caplog: pytest.LogCaptureFixture,
    snapshot: Snapshot,
):
    """Test tap output when a child stream is deselected."""
    child_stream = tap_with_deselected_child.streams["child"]

    assert not child_stream.selected

    buf = io.StringIO()
    with (
        redirect_stdout(buf),
        caplog.at_level("INFO"),
        caplog.filtering(logging.Filter(tap_with_deselected_child.name)),
    ):
        tap_with_deselected_child.sync_all()

    buf.seek(0)

    snapshot.assert_match(buf.read(), "singer.jsonl")
    snapshot.assert_match(caplog.text, "stderr.log")


@time_machine.travel(DATETIME, tick=False)
@pytest.mark.snapshot
def test_one_parent_many_children(
    caplog: pytest.LogCaptureFixture,
    snapshot: Snapshot,
):
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
            assert context is not None

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

    buf = io.StringIO()
    with (
        redirect_stdout(buf),
        caplog.at_level("INFO"),
        caplog.filtering(logging.Filter(tap.name)),
    ):
        tap.sync_all()

    buf.seek(0)

    snapshot.assert_match(buf.read(), "singer.jsonl")
    snapshot.assert_match(caplog.text, "stderr.log")


@time_machine.travel(DATETIME, tick=False)
@pytest.mark.snapshot
def test_preprocess_context_removes_large_payload(
    caplog: pytest.LogCaptureFixture,
    snapshot: Snapshot,
):
    """Test that preprocess_context can remove large payloads from parent context."""

    class ParentWithLargePayload(Stream):
        """A parent stream that passes large data in context."""

        name = "parent_large"
        schema: t.ClassVar[dict] = {
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "name": {"type": "string"},
            },
        }

        def get_child_context(
            self,
            record: dict,
            context: dict | None,  # noqa: ARG002
        ) -> dict | None:
            """Create context with large payload for child streams."""
            return {
                "parent_id": record["id"],
                "parent_name": record["name"],
                # Simulate large payload that should be removed
                "large_payload": list(range(1, 1001)),
            }

        def get_records(self, context: dict | None):  # noqa: ARG002
            """Get dummy records."""
            yield {"id": 1, "name": "Parent A"}
            yield {"id": 2, "name": "Parent B"}

    class ChildWithPreprocess(Stream):
        """A child stream that preprocesses context to remove large payloads."""

        name = "child_preprocessed"
        schema: t.ClassVar[dict] = {
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "parent_id": {"type": "integer"},
                "parent_name": {"type": "string"},
                "sum": {"type": "integer"},
            },
        }
        parent_stream_type = ParentWithLargePayload

        def __init__(self, *args: t.Any, **kwargs: t.Any) -> None:
            super().__init__(*args, **kwargs)
            self._numbers = []

        def set_numbers(self, numbers: list[int]) -> None:
            self._numbers = numbers

        def preprocess_context(self, context: dict) -> dict:
            """Remove large payload from parent context."""
            self.set_numbers(context.pop("large_payload", []))
            return context

        def get_records(self, context: dict | None):
            """Get dummy records."""
            # Verify that large_payload was removed
            assert context is not None
            assert "large_payload" not in context
            assert "parent_id" in context
            assert "parent_name" in context

            yield {
                "id": 1,
                "parent_id": context["parent_id"],
                "parent_name": context["parent_name"],
                "sum": sum(self._numbers),
            }

    class TapWithPreprocess(Tap):
        """A tap testing preprocess_context functionality."""

        name = "tap-preprocess"

        def discover_streams(self):
            """Discover streams."""
            return [
                ParentWithLargePayload(self),
                ChildWithPreprocess(self),
            ]

    tap = TapWithPreprocess()

    buf = io.StringIO()
    with (
        redirect_stdout(buf),
        caplog.at_level("INFO"),
        caplog.filtering(logging.Filter(tap.name)),
    ):
        tap.sync_all()

    buf.seek(0)

    output = buf.read()

    # Verify that large_payload doesn't appear in the output
    assert "large_payload" not in output
    assert '"data": "xxx' not in output

    snapshot.assert_match(output, "singer.jsonl")
    snapshot.assert_match(caplog.text, "stderr.log")
