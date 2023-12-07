from __future__ import annotations

import datetime

import time_machine

from tests.conftest import BatchSinkMock, TargetMock


def test_sdc_metadata():
    with time_machine.travel(
        datetime.datetime(2023, 1, 1, tzinfo=datetime.timezone.utc),
        tick=False,
    ):
        target = TargetMock()

    sink = BatchSinkMock(
        target,
        "users",
        {"type": "object", "properties": {"id": {"type": "integer"}}},
        ["id"],
    )

    record_message = {
        "type": "RECORD",
        "stream": "users",
        "record": {"id": 1},
        "time_extracted": "2021-01-01T00:00:00+00:00",
        "version": 100,
    }
    record = record_message["record"]

    with time_machine.travel(
        datetime.datetime(2023, 1, 1, 0, 5, tzinfo=datetime.timezone.utc),
        tick=False,
    ):
        sink._add_sdc_metadata_to_record(record, record_message, {})

    assert record == {
        "id": 1,
        "_sdc_extracted_at": "2021-01-01T00:00:00+00:00",
        "_sdc_received_at": "2023-01-01T00:05:00+00:00",
        "_sdc_batched_at": "2023-01-01T00:05:00+00:00",
        "_sdc_deleted_at": None,
        "_sdc_sequence": 1672531500000,
        "_sdc_table_version": 100,
        "_sdc_sync_started_at": 1672531200000,
    }

    sink._add_sdc_metadata_to_schema()
    assert sink.schema == {
        "type": "object",
        "properties": {
            "id": {"type": "integer"},
            "_sdc_extracted_at": {"type": ["null", "string"], "format": "date-time"},
            "_sdc_received_at": {"type": ["null", "string"], "format": "date-time"},
            "_sdc_batched_at": {"type": ["null", "string"], "format": "date-time"},
            "_sdc_deleted_at": {"type": ["null", "string"], "format": "date-time"},
            "_sdc_sequence": {"type": ["null", "integer"]},
            "_sdc_table_version": {"type": ["null", "integer"]},
            "_sdc_sync_started_at": {"type": ["null", "integer"]},
        },
    }
