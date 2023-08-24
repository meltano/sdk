from __future__ import annotations

import copy
import time

import pytest

from singer_sdk.exceptions import (
    MissingKeyPropertiesError,
    RecordsWithoutSchemaException,
)
from singer_sdk.helpers.capabilities import PluginCapabilities
from tests.conftest import BatchSinkMock, SQLSinkMock, SQLTargetMock, TargetMock


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
        PluginCapabilities.BATCH,
    ]

    assert "stream_maps" in about.settings["properties"]
    assert "stream_map_config" in about.settings["properties"]
    assert "flattening_enabled" in about.settings["properties"]
    assert "flattening_max_depth" in about.settings["properties"]
    assert "batch_config" in about.settings["properties"]
    assert "add_record_metadata" in about.settings["properties"]
    assert "batch_size_rows" in about.settings["properties"]
    assert "batch_wait_limit_seconds" in about.settings["properties"]
    assert "batch_dynamic_management" in about.settings["properties"]


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


def test_batch_wait_limit_seconds():
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
    target_set = TargetMock(config={"batch_wait_limit_seconds": 1})
    sink_set = BatchSinkMock(
        target=target_set,
        stream_name="foo",
        schema=input_schema_1,
        key_properties=key_properties,
    )
    assert sink_set.stream_name == "foo"
    assert sink_set.batch_wait_limit_seconds == 1
    assert sink_set.sink_timer is not None
    assert sink_set.sink_timer.start_time is not None
    assert sink_set.batch_size_rows is None
    assert sink_set.max_size == 10000
    assert sink_set.is_too_old() is False
    time.sleep(1.1)
    assert sink_set.is_too_old() is True
    assert sink_set.sink_timer.start_time > 1.1
    assert sink_set.sink_timer.stop_time is None
    assert sink_set.sink_timer.lap_time is None
    assert sink_set.sink_timer.perf_diff == 0.0
    sink_set._lap_manager()
    assert sink_set.sink_timer.start_time > 0.0
    assert sink_set.sink_timer.stop_time is None
    assert sink_set.sink_timer.lap_time > 1.0
    assert sink_set.sink_timer.perf_diff < 0.0


def test_batch_dynamic_management():
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
    target_set = TargetMock(
        config={
            "batch_size_rows": 100000,
            "batch_wait_limit_seconds": 2,
            "batch_dynamic_management": True,
        },
    )
    sink_set = BatchSinkMock(
        target=target_set,
        stream_name="foo",
        schema=input_schema_1,
        key_properties=key_properties,
    )
    assert sink_set.stream_name == "foo"
    assert sink_set.batch_dynamic_management is True
    assert sink_set.batch_wait_limit_seconds == 2
    assert sink_set.sink_timer is not None
    assert sink_set.sink_timer.start_time is not None
    assert sink_set.batch_size_rows == 100000
    assert sink_set.max_size == 100000
    assert sink_set.sink_timer.sink_max_size == 100
    time.sleep(1.1)
    sink_set._lap_manager()
    assert sink_set.sink_timer.start_time > 0.0
    assert sink_set.sink_timer.stop_time is None
    assert sink_set.sink_timer.lap_time > 1.0
    assert sink_set.sink_timer.lap_time < 2.0
    assert sink_set.sink_timer.perf_diff > sink_set.sink_timer.perf_diff_allowed_max
    assert sink_set.sink_timer.sink_max_size == 200
    time.sleep(3.1)
    sink_set._lap_manager()
    assert sink_set.sink_timer.start_time > 0.0
    assert sink_set.sink_timer.stop_time is None
    assert sink_set.sink_timer.lap_time > 1.0
    assert sink_set.sink_timer.perf_diff < sink_set.sink_timer.perf_diff_allowed_min
    assert sink_set.sink_timer.sink_max_size == 190


@pytest.mark.parametrize(
    "config,rows,wait_time,expected",
    [
        pytest.param(
            {},
            10001,
            None,
            True,
            id="default_config_true",
        ),
        pytest.param(
            {},
            100,
            None,
            False,
            id="default_config_false",
        ),
        pytest.param(
            {"batch_size_rows": 100000},
            100001,
            None,
            True,
            id="batch_size_rows_only_true",
        ),
        pytest.param(
            {"batch_size_rows": 100000},
            1,
            None,
            False,
            id="batch_size_rows_only_false",
        ),
        pytest.param(
            {"batch_wait_limit_seconds": 1},
            None,
            2,
            True,
            id="wait_limit_seconds_only_true",
        ),
        pytest.param(
            {"batch_wait_limit_seconds": 2},
            None,
            1,
            False,
            id="wait_limit_seconds_only_false",
        ),
        pytest.param(
            {"batch_size_rows": 100000, "batch_wait_limit_seconds": 2},
            100001,
            1,
            True,
            id="batch_rows_and_wait_limit_seconds_true_rows",
        ),
        pytest.param(
            {"batch_size_rows": 100000, "batch_wait_limit_seconds": 1},
            100,
            2,
            True,
            id="batch_rows_and_wait_limit_seconds_true_wait",
        ),
        pytest.param(
            {"batch_size_rows": 100000, "batch_wait_limit_seconds": 2},
            100,
            1,
            False,
            id="batch_rows_and_wait_limit_seconds_false",
        ),
    ],
)
def test_is_full(
    config: str,
    rows: int | None,
    wait_time: int | None,
    expected: bool,
):
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
    target = TargetMock(config=config)
    sink = BatchSinkMock(
        target=target,
        stream_name="foo",
        schema=input_schema_1,
        key_properties=key_properties,
    )
    if rows is not None:
        sink._batch_records_read = rows
    if wait_time is not None:
        time.sleep(wait_time)
    assert sink.is_full == expected
