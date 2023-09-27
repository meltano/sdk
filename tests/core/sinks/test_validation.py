from __future__ import annotations

import datetime

from tests.conftest import BatchSinkMock, TargetMock


def test_validate_record():
    target = TargetMock()
    sink = BatchSinkMock(
        target,
        "users",
        {
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "created_at": {"type": "string", "format": "date-time"},
                "invalid_datetime": {"type": "string", "format": "date-time"},
            },
        },
        ["id"],
    )

    record_message = {
        "type": "RECORD",
        "stream": "users",
        "record": {
            "id": 1,
            "created_at": "2021-01-01T00:00:00+00:00",
            "missing_datetime": "2021-01-01T00:00:00+00:00",
            "invalid_datetime": "not a datetime",
        },
        "time_extracted": "2021-01-01T00:00:00+00:00",
        "version": 100,
    }
    record = record_message["record"]
    updated_record = sink._validate_and_parse(record)
    assert updated_record["created_at"] == datetime.datetime(
        2021,
        1,
        1,
        0,
        0,
        tzinfo=datetime.timezone.utc,
    )
    assert updated_record["missing_datetime"] == "2021-01-01T00:00:00+00:00"
    assert updated_record["invalid_datetime"] == "9999-12-31 23:59:59.999999"
