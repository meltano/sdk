"""Tests for the new load_record() and load_batch() methods."""

from __future__ import annotations

import pytest

from singer_sdk.sinks import BatchContext, BatchSink, RecordSink
from singer_sdk.target_base import Target


class DummyTarget(Target):
    """Dummy target for testing."""

    name = "dummy-target"


class ModernBatchSink(BatchSink):
    """Example sink using the new load_batch() method."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.loaded_batches: list[list[dict]] = []

    def load_batch(self, batch: BatchContext) -> None:
        """Load using the new type-safe method."""
        self.loaded_batches.append(batch.records.copy())


class LegacyBatchSink(BatchSink):
    """Example sink using the old process_batch() method."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.loaded_batches: list[list[dict]] = []

    def process_batch(self, context: dict) -> None:
        """Load using the legacy method."""
        self.loaded_batches.append(context["records"].copy())


class ModernRecordSink(RecordSink):
    """Example sink using the new load_record() method."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.loaded_records: list[dict] = []

    def load_record(self, record: dict) -> None:
        """Load using the new simplified method."""
        self.loaded_records.append(record)


class LegacyRecordSink(RecordSink):
    """Example sink using the old process_record() method."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.loaded_records: list[dict] = []

    def process_record(self, record: dict, context: dict) -> None:
        """Load using the legacy method."""
        self.loaded_records.append(record)


def test_modern_batch_sink():
    """Test that the new load_batch() method works."""
    target = DummyTarget()
    sink = ModernBatchSink(
        target=target,
        stream_name="test_stream",
        schema={"properties": {"id": {"type": "integer"}}},
        key_properties=["id"],
    )

    # Create a batch context
    batch = BatchContext(
        batch_id="test-batch",
        batch_start_time=__import__("datetime").datetime.now(
            __import__("datetime").timezone.utc
        ),
        records=[{"id": 1}, {"id": 2}, {"id": 3}],
    )

    # Call load_batch directly
    sink.load_batch(batch)

    # Verify records were loaded
    assert len(sink.loaded_batches) == 1
    assert sink.loaded_batches[0] == [{"id": 1}, {"id": 2}, {"id": 3}]


def test_legacy_batch_sink():
    """Test that the old process_batch() method still works."""
    target = DummyTarget()
    sink = LegacyBatchSink(
        target=target,
        stream_name="test_stream",
        schema={"properties": {"id": {"type": "integer"}}},
        key_properties=["id"],
    )

    # Call process_batch with legacy dict context
    context = {"records": [{"id": 1}, {"id": 2}, {"id": 3}]}
    sink.process_batch(context)

    # Verify records were loaded
    assert len(sink.loaded_batches) == 1
    assert sink.loaded_batches[0] == [{"id": 1}, {"id": 2}, {"id": 3}]


def test_modern_record_sink():
    """Test that the new load_record() method works."""
    target = DummyTarget()
    sink = ModernRecordSink(
        target=target,
        stream_name="test_stream",
        schema={"properties": {"id": {"type": "integer"}}},
        key_properties=["id"],
    )

    # Call load_record directly
    sink.load_record({"id": 1})
    sink.load_record({"id": 2})

    # Verify records were loaded
    assert len(sink.loaded_records) == 2
    assert sink.loaded_records == [{"id": 1}, {"id": 2}]


def test_legacy_record_sink():
    """Test that the old process_record() method still works."""
    target = DummyTarget()
    sink = LegacyRecordSink(
        target=target,
        stream_name="test_stream",
        schema={"properties": {"id": {"type": "integer"}}},
        key_properties=["id"],
    )

    # Call process_record with legacy signature
    sink.process_record({"id": 1}, {})
    sink.process_record({"id": 2}, {})

    # Verify records were loaded
    assert len(sink.loaded_records) == 2
    assert sink.loaded_records == [{"id": 1}, {"id": 2}]


def test_batch_context_conversion():
    """Test BatchContext to/from dict conversion."""
    import datetime

    original_dict = {
        "batch_id": "test-123",
        "batch_start_time": datetime.datetime.now(datetime.timezone.utc),
        "records": [{"id": 1}, {"id": 2}],
        "custom_key": "custom_value",
    }

    # Convert to BatchContext
    batch = BatchContext.from_legacy_dict(original_dict)

    assert batch.batch_id == "test-123"
    assert batch.records == [{"id": 1}, {"id": 2}]
    assert batch.metadata["custom_key"] == "custom_value"

    # Convert back to dict
    result_dict = batch.to_legacy_dict()

    assert result_dict["batch_id"] == original_dict["batch_id"]
    assert result_dict["records"] == original_dict["records"]
    assert result_dict["custom_key"] == "custom_value"


def test_batch_sink_without_override_raises_error():
    """Test that BatchSink raises error if neither method is overridden."""

    class IncompleteBackSink(BatchSink):
        """A sink that doesn't override either method."""

    target = DummyTarget()
    sink = IncompleteBackSink(
        target=target,
        stream_name="test_stream",
        schema={"properties": {"id": {"type": "integer"}}},
        key_properties=["id"],
    )

    with pytest.raises(NotImplementedError, match="must override either"):
        sink.process_batch({"records": []})


def test_record_sink_without_override_raises_error():
    """Test that RecordSink raises error if neither method is overridden."""

    class IncompleteRecordSink(RecordSink):
        """A sink that doesn't override either method."""

    target = DummyTarget()
    sink = IncompleteRecordSink(
        target=target,
        stream_name="test_stream",
        schema={"properties": {"id": {"type": "integer"}}},
        key_properties=["id"],
    )

    with pytest.raises(NotImplementedError, match="must override either"):
        sink.process_record({"id": 1}, {})
