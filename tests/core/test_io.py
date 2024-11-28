"""Test IO operations."""

from __future__ import annotations

import decimal
import io
import itertools
import json
import logging
from contextlib import nullcontext, redirect_stdout
from textwrap import dedent

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


def test_listen():
    reader = DummyReader()
    input_lines = io.StringIO(
        dedent("""\
        {"type": "SCHEMA", "stream": "users", "schema": {"type": "object", "properties": {"id": {"type": "integer"}, "value": {"type": "number"}}}}
        {"type": "RECORD", "stream": "users", "record": {"id": 1, "value": 1.23}}
        {"type": "RECORD", "stream": "users", "record": {"id": 2, "value": 2.34}}
        {"type": "STATE", "value": {"bookmarks": {"users": {"id": 2}}}}
        {"type": "SCHEMA", "stream": "batches", "schema": {"type": "object", "properties": {"id": {"type": "integer"}, "value": {"type": "number"}}}}
        {"type": "BATCH", "stream": "batches", "encoding": {"format": "jsonl", "compression": "gzip"}, "manifest": ["file1.jsonl.gz", "file2.jsonl.gz"]}
        {"type": "STATE", "value": {"bookmarks": {"users": {"id": 2}, "batches": {"id": 1000000}}}}
    """)  # noqa: E501
    )
    reader.listen(input_lines)


def test_listen_unknown_message():
    reader = DummyReader()
    input_lines = io.StringIO('{"type": "UNKNOWN"}\n')
    with pytest.raises(ValueError, match="Unknown message type"):
        reader.listen(input_lines)


def test_listen_error(caplog: pytest.LogCaptureFixture):
    class ErrorReader(DummyReader):
        def _process_record_message(self, message_dict: dict) -> None:  # noqa: ARG002
            msg = "Bad record"
            raise ValueError(msg)

    message = RecordMessage(
        stream="users",
        record={"id": 1, "value": 1.23},
    )

    input_lines = io.StringIO(json.dumps(message.to_dict()) + "\n")

    reader = ErrorReader()
    with caplog.at_level(logging.DEBUG), pytest.raises(ValueError, match="Bad record"):
        reader.listen(input_lines)

    assert caplog.records

    message = caplog.records[0].message
    assert "Failed while processing Singer message" in message


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
