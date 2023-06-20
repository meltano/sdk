from __future__ import annotations

from tests.conftest import BatchSinkMock, TargetMock


def test_get_sink():
    input_schema = {
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
    target = TargetMock(config={"add_record_metadata": True})
    sink = BatchSinkMock(target, "foo", input_schema, key_properties)
    target._sinks_active["foo"] = sink
    sink_returned = target.get_sink(
        "foo", schema=input_schema, key_properties=key_properties
    )
    assert sink_returned == sink
