from __future__ import annotations

import copy
import signal
import sys
import typing as t
from unittest import mock

import pytest

from singer_sdk.exceptions import (
    MissingKeyPropertiesError,
    RecordsWithoutSchemaException,
)
from singer_sdk.helpers.capabilities import PluginCapabilities, TargetCapabilities
from tests.conftest import BatchSinkMock, SQLSinkMock, SQLTargetMock, TargetMock

if sys.version_info >= (3, 12):
    from typing import override  # noqa: ICN003
else:
    from typing_extensions import override


def test_get_sink():
    input_schema_1 = {
        "properties": {
            "id": {
                "type": ["string", "null"],
            },
            "col_ts": {
                "format": "date-time",
                "type": ["string", "null"],
            },
        },
    }
    input_schema_2 = copy.deepcopy(input_schema_1)
    key_properties = []
    target = TargetMock(config={"add_record_metadata": True})
    sink = BatchSinkMock(target, "foo", input_schema_1, key_properties)
    target._sinks_active["foo"] = sink
    sink_returned = target.get_sink(
        "foo",
        schema=input_schema_2,
        key_properties=key_properties,
    )
    assert sink_returned == sink


def test_validate_record():
    target = TargetMock()
    sink = BatchSinkMock(
        target=target,
        stream_name="test",
        schema={
            "properties": {
                "id": {"type": ["integer"]},
                "name": {"type": ["string"]},
            },
        },
        key_properties=["id"],
    )

    # Test valid record
    sink._singer_validate_message({"id": 1, "name": "test"})

    # Test invalid record
    with pytest.raises(MissingKeyPropertiesError):
        sink._singer_validate_message({"name": "test"})


def test_target_about_info():
    target = TargetMock()
    about = target._get_about_info()

    assert about.capabilities == [
        PluginCapabilities.ABOUT,
        PluginCapabilities.STREAM_MAPS,
        PluginCapabilities.FLATTENING,
        PluginCapabilities.STRUCTURED_LOGGING,
        TargetCapabilities.VALIDATE_RECORDS,
        PluginCapabilities.BATCH,
    ]

    assert "stream_maps" in about.settings["properties"]
    assert "stream_map_config" in about.settings["properties"]
    assert "flattening_enabled" in about.settings["properties"]
    assert "flattening_max_depth" in about.settings["properties"]
    assert "batch_config" in about.settings["properties"]
    assert "add_record_metadata" in about.settings["properties"]
    assert "batch_size_rows" in about.settings["properties"]


def test_sql_get_sink():
    input_schema_1 = {
        "properties": {
            "id": {
                "type": ["string", "null"],
            },
            "col_ts": {
                "format": "date-time",
                "type": ["string", "null"],
            },
        },
    }
    input_schema_2 = copy.deepcopy(input_schema_1)
    key_properties = []
    target = SQLTargetMock(config={"sqlalchemy_url": "sqlite:///"})
    sink = SQLSinkMock(
        target=target,
        stream_name="foo",
        schema=input_schema_1,
        key_properties=key_properties,
        connector=target.target_connector,
    )
    target._sinks_active["foo"] = sink
    sink_returned = target.get_sink(
        "foo",
        schema=input_schema_2,
        key_properties=key_properties,
    )
    assert sink_returned is sink


def test_add_sqlsink_and_get_sink():
    input_schema_1 = {
        "properties": {
            "id": {
                "type": ["string", "null"],
            },
            "col_ts": {
                "format": "date-time",
                "type": ["string", "null"],
            },
        },
    }
    input_schema_2 = copy.deepcopy(input_schema_1)
    key_properties = []
    target = SQLTargetMock(config={"sqlalchemy_url": "sqlite:///"})
    sink = target.add_sqlsink(
        "foo",
        schema=input_schema_2,
        key_properties=key_properties,
    )

    sink_returned = target.get_sink(
        "foo",
    )

    assert sink_returned is sink

    # Test invalid call
    with pytest.raises(RecordsWithoutSchemaException):
        target.get_sink(
            "bar",
        )


def test_create_sink_override():
    """Target.create_sink() can be overridden to inject extra constructor args."""
    injected = {}

    class CustomSink(BatchSinkMock):
        def __init__(self, *args, extra=None, **kwargs):
            super().__init__(*args, **kwargs)
            injected["extra"] = extra

    class CustomTarget(TargetMock):
        @override
        def create_sink(self, *, stream_name, schema, key_properties=None):
            return CustomSink(
                target=self,
                stream_name=stream_name,
                schema=schema,
                key_properties=key_properties,
                extra="injected_value",
            )

    schema = {"properties": {"id": {"type": ["integer"]}}}
    target = CustomTarget()
    sink = target.add_sink("test_stream", schema, [])
    assert isinstance(sink, CustomSink)
    assert injected["extra"] == "injected_value"


def test_sql_create_sink_override():
    """SQLTarget.create_sink() can be overridden to inject extra constructor args."""
    injected = {}

    class CustomSQLSink(SQLSinkMock):
        def __init__(self, *args, extra=None, **kwargs):
            super().__init__(*args, **kwargs)
            injected["extra"] = extra

    class CustomSQLTarget(SQLTargetMock):
        @override
        def create_sink(self, *, stream_name, schema, key_properties=None):
            return CustomSQLSink(
                target=self,
                stream_name=stream_name,
                schema=schema,
                key_properties=key_properties,
                connector=self.target_connector,
                extra="sql_injected",
            )

    schema = {"properties": {"id": {"type": ["integer"]}}}
    target = CustomSQLTarget(config={"sqlalchemy_url": "sqlite:///"})
    sink = target.add_sqlsink("test_stream", schema, [])
    assert isinstance(sink, CustomSQLSink)
    assert injected["extra"] == "sql_injected"


def test_batch_size_rows_and_max_size():
    input_schema_1 = {
        "properties": {
            "id": {
                "type": ["string", "null"],
            },
            "col_ts": {
                "format": "date-time",
                "type": ["string", "null"],
            },
        },
    }
    key_properties = []
    target_default = TargetMock()
    sink_default = BatchSinkMock(
        target=target_default,
        stream_name="foo",
        schema=input_schema_1,
        key_properties=key_properties,
    )
    target_set = TargetMock(config={"batch_size_rows": 100000})
    sink_set = BatchSinkMock(
        target=target_set,
        stream_name="bar",
        schema=input_schema_1,
        key_properties=key_properties,
    )
    assert sink_default.stream_name == "foo"
    assert sink_default._batch_size_rows is None
    assert sink_default.batch_size_rows is None
    assert sink_default.max_size == 10000
    assert sink_set.stream_name == "bar"
    assert sink_set._batch_size_rows == 100000
    assert sink_set.batch_size_rows == 100000
    assert sink_set.max_size == 100000


def test_duplicate_termination_signal_does_not_redrain():
    """A second signal arriving mid-drain is ignored and sinks drain once."""
    target = TargetMock()
    drain_calls: list[dict] = []

    def fake_drain_all(**kwargs) -> None:
        drain_calls.append(kwargs)
        # simulate the duplicate signal arriving while the drain is in flight,
        # e.g. delivered by the OS process group and forwarded by Meltano
        target._handle_termination(signal.SIGINT, None)

    with (
        mock.patch.object(target, "drain_all", side_effect=fake_drain_all),
        pytest.raises(SystemExit) as exc_info,
    ):
        target._handle_termination(signal.SIGTERM, None)

    assert exc_info.value.code == 0
    assert len(drain_calls) == 1


def _make_signalled_sink(target: TargetMock, stream_name: str) -> BatchSinkMock:
    """Register a sink whose first `process_batch()` call fires a SIGTERM.

    This simulates the first signal arriving mid-`process_batch()`, before the
    original call has committed its records, as described in
    https://github.com/meltano/sdk/issues/3775.
    """
    schema = {"properties": {"id": {"type": "integer"}}}
    sink = t.cast(
        "BatchSinkMock",
        target.get_sink(stream_name, schema=schema, key_properties=["id"]),
    )

    def process_batch(context: dict) -> None:
        if not target.signalled:
            target.signalled = True
            target._handle_termination(signal.SIGTERM, None)
        target.records_written.extend(context["records"])
        target.num_batches_processed += 1

    sink.process_batch = process_batch

    for i in range(3):
        context = sink._get_context({"id": i})
        sink.process_record({"id": i}, context)
        sink.tally_record_read()

    return sink


def test_signal_during_final_drain_does_not_lose_records():
    """A signal mid-`process_batch()` of the final drain must not lose records.

    It must not re-enter `drain_one()` for the same sink either.
    """
    target = TargetMock()
    _make_signalled_sink(target, "foo")
    target._latest_state = {
        "bookmarks": {"foo": {"replication_key_value": 2}},
    }

    # The interrupted drain is already the end-of-pipe drain, so no further
    # forced shutdown is needed once it completes.
    target.drain_all(is_endofpipe=True)

    assert target.records_written == [{"id": 0}, {"id": 1}, {"id": 2}]
    assert target.num_batches_processed == 1
    assert target.state_messages_written[-1] == target._latest_state
    assert target._is_terminating is True


def test_signal_during_midstream_drain_defers_shutdown_then_exits():
    """A signal during a non-final drain must defer, then drain and exit.

    A SIGTERM arriving mid-drain (e.g. size/age-triggered) must let the
    in-flight batch commit, then perform a real end-of-pipe drain and exit.
    """
    target = TargetMock()
    _make_signalled_sink(target, "foo")
    target._latest_state = {
        "bookmarks": {"foo": {"replication_key_value": 2}},
    }

    with pytest.raises(SystemExit) as exc_info:
        target.drain_all(is_endofpipe=False)

    assert exc_info.value.code == 0
    assert target.records_written == [{"id": 0}, {"id": 1}, {"id": 2}]
    assert target.num_batches_processed == 1
    assert target.state_messages_written[-1] == target._latest_state
