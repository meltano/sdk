from __future__ import annotations

import datetime
import io
import json
import logging
import sys
import typing as t
from collections import defaultdict
from contextlib import redirect_stdout

import pytest
import time_machine

from singer_sdk import Stream, Tap

if sys.version_info >= (3, 12):
    from typing import override  # noqa: ICN003
else:
    from typing_extensions import override

if t.TYPE_CHECKING:
    from pytest_snapshot.plugin import Snapshot

    from singer_sdk.helpers.types import Context, Record

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

    @override
    def get_child_context(
        self,
        record: Record,
        context: Context | None,
    ) -> Context | None:
        """Create context for children streams."""
        return {
            "pid": record["id"],
        }

    @override
    def get_records(self, context: Context | None):
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

    @override
    def get_records(self, context: Context | None):
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

        @override
        def get_records(
            self,
            context: Context | None,
        ) -> t.Iterable[dict | tuple[dict, dict | None]]:
            yield {"id": "1", "children": [1, 2, 3]}

        @override
        def generate_child_contexts(
            self,
            record: Record,
            context: Context | None,
        ) -> t.Iterable[Context | None]:
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

        @override
        def get_records(self, context: Context | None):
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

        @override
        def get_child_context(
            self,
            record: Record,
            context: Context | None,
        ) -> dict | None:
            """Create context with large payload for child streams."""
            return {
                "parent_id": record["id"],
                "parent_name": record["name"],
                # Simulate large payload that should be removed
                "large_payload": list(range(1, 1001)),
            }

        @override
        def get_records(self, context: Context | None):
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

        @override
        def preprocess_context(self, context: Context) -> Context:
            """Remove large payload from parent context."""
            self.set_numbers(context.pop("large_payload", []))
            return context

        @override
        def get_records(self, context: Context | None):
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


def test_parent_records_emitted_when_child_hits_record_limit():
    """Parent records are written before child sync so they appear even if a child
    stream aborts after reaching max_records_limit (dry-run record cap)."""

    class ParentStream(Stream):
        name = "parent_limited"
        schema: t.ClassVar[dict] = {
            "type": "object",
            "properties": {"id": {"type": "integer"}},
        }

        def get_child_context(
            self,
            record: dict,
            context: dict | None,  # noqa: ARG002
        ) -> dict | None:
            return {"pid": record["id"]}

        def get_records(self, context: dict | None):  # noqa: ARG002
            yield {"id": 1}
            yield {"id": 2}

    class ChildStream(Stream):
        name = "child_limited"
        schema: t.ClassVar[dict] = {
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "pid": {"type": "integer"},
            },
        }
        parent_stream_type = ParentStream

        def get_records(self, context: dict | None):  # noqa: ARG002
            # More records than the dry-run limit (3 > 2)
            yield {"id": 1}
            yield {"id": 2}
            yield {"id": 3}

    class SiblingStream(Stream):
        name = "sibling"
        schema: t.ClassVar[dict] = {
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "pid": {"type": "integer"},
            },
        }
        parent_stream_type = ParentStream

        def get_records(self, context: dict | None):  # noqa: ARG002
            for i in range(10):
                yield {"id": i + 1}

    class TapLimited(Tap):
        name = "tap-limited"

        def discover_streams(self):
            return [ParentStream(self), ChildStream(self), SiblingStream(self)]

    tap = TapLimited()
    buf = io.StringIO()
    with redirect_stdout(buf):
        # Limit child to 2 records; the 3rd would trigger AbortedSyncPausedException
        tap.run_sync_dry_run(dry_run_record_limit=2)

    buf.seek(0)
    messages = [json.loads(line) for line in buf.read().splitlines() if line]

    tally: dict[str, int] = defaultdict(int)
    for m in messages:
        if m["type"] == "RECORD":
            tally[m["stream"]] += 1

    msg = "At least one parent record must be emitted even when the child stream hits its dry-run record limit."  # noqa: E501
    assert tally["parent_limited"] >= 1, msg

    msg = "Only 2 (for each parent) child records should be emitted"
    assert tally["child_limited"] == 4, msg

    msg = "Sibling records should also be emitted"
    assert tally["sibling"] == 4, msg
