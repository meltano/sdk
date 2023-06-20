from __future__ import annotations

import copy

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
