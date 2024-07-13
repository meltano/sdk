"""Test IO operations."""

from __future__ import annotations

import decimal
import io
import itertools
import json
from contextlib import nullcontext, redirect_stdout

import pytest

from singer_sdk._singerlib import RecordMessage
from singer_sdk._singerlib.exceptions import InvalidInputLine
from singer_sdk.io_base import SingerReader, SingerWriter


class DummyReader(SingerReader):
    def _process_activate_version_message(self, message_dict: dict) -> None:
        pass

    def _process_batch_message(self, message_dict: dict) -> None:
        pass

    def _process_record_message(self, message_dict: dict) -> None:
        pass

    def _process_schema_message(self, message_dict: dict) -> None:
        pass

    def _process_state_message(self, message_dict: dict) -> None:
        pass


@pytest.mark.parametrize(
    "line,expected,exception",
    [
        pytest.param(
            "not-valid-json",
            None,
            pytest.raises(InvalidInputLine),
            id="unparsable",
        ),
        pytest.param(
            '{"type": "RECORD", "stream": "users", "record": {"id": 1, "value": 1.23}}',
            {
                "type": "RECORD",
                "stream": "users",
                "record": {"id": 1, "value": decimal.Decimal("1.23")},
            },
            nullcontext(),
            id="record",
        ),
    ],
)
def test_deserialize(line, expected, exception):
    reader = DummyReader()
    with exception:
        assert reader.deserialize_json(line) == expected


def test_write_message():
    writer = SingerWriter()
    message = RecordMessage(
        stream="test",
        record={"id": 1, "name": "test"},
    )
    with redirect_stdout(io.StringIO()) as out:
        writer.write_message(message)

    assert out.getvalue() == (
        '{"type":"RECORD","stream":"test","record":{"id":1,"name":"test"}}\n'
    )


# Benchmark Tests


@pytest.fixture
def bench_record():
    return {
        "stream": "users",
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


def test_bench_format_message(benchmark, bench_record_message):
    """Run benchmark for Sink._validator method validate."""
    number_of_runs = 1000

    writer = SingerWriter()

    def run_format_message():
        for record in itertools.repeat(bench_record_message, number_of_runs):
            writer.format_message(record)

    benchmark(run_format_message)


def test_bench_deserialize_json(benchmark, bench_encoded_record):
    """Run benchmark for Sink._validator method validate."""
    number_of_runs = 1000

    reader = DummyReader()

    def run_deserialize_json():
        for record in itertools.repeat(bench_encoded_record, number_of_runs):
            reader.deserialize_json(record)

    benchmark(run_deserialize_json)
