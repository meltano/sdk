"""Test IO operations for msgspec Singer reader and writer."""  # noqa: INP001

from __future__ import annotations

import decimal
import io
import itertools
from contextlib import nullcontext, redirect_stdout
from textwrap import dedent

import pytest

from singer_sdk._singerlib import RecordMessage
from singer_sdk._singerlib.encoding._msgspec import (
    MsgSpecReader,
    MsgSpecWriter,
    dec_hook,
    enc_hook,
)
from singer_sdk._singerlib.exceptions import InvalidInputLine


@pytest.mark.parametrize(
    "test_type,test_value,expected_value,expected_type",
    [
        pytest.param(
            int,
            1,
            "1",
            str,
            id="int-to-str",
        ),
    ],
)
def test_dec_hook(test_type, test_value, expected_value, expected_type):
    returned = dec_hook(type=test_type, obj=test_value)
    returned_type = type(returned)

    assert returned == expected_value
    assert returned_type == expected_type


@pytest.mark.parametrize(
    "test_value,expected_value",
    [
        pytest.param(
            1,
            "1",
            id="int-to-str",
        ),
    ],
)
def test_enc_hook(test_value, expected_value):
    returned = enc_hook(obj=test_value)

    assert returned == expected_value


class DummyReader(MsgSpecReader):
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


def test_write_message():
    writer = MsgSpecWriter()
    message = RecordMessage(
        stream="test",
        record={"id": 1, "name": "test"},
    )
    with redirect_stdout(io.TextIOWrapper(io.BytesIO())) as out:  # noqa: PLW1514
        writer.write_message(message)

    out.seek(0)
    assert out.read() == (
        '{"type":"RECORD","stream":"test","record":{"id":1,"name":"test"}}\n'
    )


# Benchmark Tests


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

    reader = DummyReader()

    def run_deserialize_json():
        for record in itertools.repeat(bench_encoded_record, number_of_runs):
            reader.deserialize_json(record)

    benchmark(run_deserialize_json)
