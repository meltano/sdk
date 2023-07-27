from __future__ import annotations

import copy

import pytest

from singer_sdk.exceptions import MissingKeyPropertiesError
from singer_sdk.helpers.capabilities import PluginCapabilities
from tests.conftest import BatchSinkMock, TargetMock


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
