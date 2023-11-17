from __future__ import annotations

import datetime

import pytest

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

    record = {
        "id": 1,
        "created_at": "2021-01-01T00:00:00+00:00",
        "missing_datetime": "2021-01-01T00:00:00+00:00",
        "invalid_datetime": "not a datetime",
    }
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


@pytest.fixture
def bench_sink() -> BatchSinkMock:
    target = TargetMock()
    return BatchSinkMock(
        target,
        "users",
        {
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "created_at": {"type": "string", "format": "date-time"},
                "updated_at": {"type": "string", "format": "date-time"},
                "deleted_at": {"type": "string", "format": "date-time"},
            },
        },
        ["id"],
    )


@pytest.fixture
def bench_record():
    return {
        "id": 1,
        "created_at": "2021-01-01T00:08:00-07:00",
        "updated_at": "2022-01-02T00:09:00-07:00",
        "deleted_at": "2023-01-03T00:10:00.0000",
    }


def test_bench_parse_timestamps_in_record(benchmark, bench_sink, bench_record):
    """Run benchmark tests using the "repositories" stream."""
    record_size_scale = 10000

    sink: BatchSinkMock = bench_sink
    record: dict = bench_record

    def run_parse_timestamps_in_record():
        for _ in range(record_size_scale):
            _ = sink._parse_timestamps_in_record(
                record, sink.schema, sink.datetime_error_treatment
            )

    benchmark(run_parse_timestamps_in_record)


def test_bench_validate_and_parse(benchmark, bench_sink, bench_record):
    """Run benchmark tests using the "repositories" stream."""
    record_size_scale = 10000

    sink: BatchSinkMock = bench_sink
    record: dict = bench_record

    def run_validate_and_parse():
        for _ in range(record_size_scale):
            _ = sink._validate_and_parse(record)

    benchmark(run_validate_and_parse)
