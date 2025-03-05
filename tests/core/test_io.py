"""Test IO operations."""

from __future__ import annotations

import itertools
import json

import pytest

from singer_sdk._singerlib import RecordMessage
from singer_sdk.contrib.msgspec import MsgSpecReader, MsgSpecWriter

# IO Benchmarks


@pytest.fixture
def bench_record():
    return {
        "stream": "users",
        "type": "RECORD",
        "record": {
            "Id": 1,
            "created_at": "2021-01-01T00:08:00-07:00",
            "updated_at": "2022-01-02T00:09:00-07:00",
            "deleted_at": "2023-01-03T00:10:00-07:00",
            "value": 1.23,
            "RelatedtId": 32412,
            "TypeId": 1,
        },
        "time_extracted": "2023-01-01T11:00:00.00000-07:00",
    }


@pytest.fixture
def bench_record_message(bench_record):
    return RecordMessage.from_dict(bench_record)


@pytest.fixture
def bench_encoded_record(bench_record):
    return json.dumps(bench_record)


def test_bench_format_message(benchmark, bench_record_message: RecordMessage):
    """Run benchmark for Sink._validator method validate."""
    number_of_runs = 1000

    writer = MsgSpecWriter()

    def run_format_message():
        for record in itertools.repeat(bench_record_message, number_of_runs):
            writer.format_message(record)

    benchmark(run_format_message)


def test_bench_deserialize_json(benchmark, bench_encoded_record: str):
    """Run benchmark for Sink._validator method validate."""
    number_of_runs = 1000
    reader = MsgSpecReader()

    def run_deserialize_json():
        for record in itertools.repeat(bench_encoded_record, number_of_runs):
            reader.deserialize_json(record)

    benchmark(run_deserialize_json)
