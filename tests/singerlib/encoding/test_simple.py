"""Test IO operations for simple Singer reader and writer."""

from __future__ import annotations

import decimal
import io
import json
from contextlib import nullcontext, redirect_stdout
from textwrap import dedent

import pytest

from singer_sdk._singerlib import RecordMessage
from singer_sdk._singerlib.exceptions import InvalidInputLine
from singer_sdk.singerlib.encoding.simple import (
    SimpleSingerReader,
    SimpleSingerWriter,
)

CALLBACKS = {
    "RECORD": lambda _: None,
    "SCHEMA": lambda _: None,
    "STATE": lambda _: None,
    "ACTIVATE_VERSION": lambda _: None,
    "BATCH": lambda _: None,
}


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
    reader = SimpleSingerReader()
    with exception:
        assert reader.deserialize_json(line) == expected


def test_process_lines():
    reader = SimpleSingerReader()
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
    reader.process_lines(input_lines, CALLBACKS)


def test_process_unknown_message():
    reader = SimpleSingerReader()
    input_lines = io.StringIO('{"type": "UNKNOWN"}\n')
    with pytest.raises(ValueError, match="Unknown message type"):
        reader.process_lines(input_lines, CALLBACKS)


def test_process_error():
    def error(message_dict: dict) -> None:  # noqa: ARG001
        msg = "Bad record"
        raise ValueError(msg)

    message = RecordMessage(
        stream="users",
        record={"id": 1, "value": 1.23},
    )
    input_lines = io.StringIO(json.dumps(message.to_dict()) + "\n")
    reader = SimpleSingerReader()

    with pytest.raises(ValueError, match="Bad record"):
        reader.process_lines(input_lines, {**CALLBACKS, "RECORD": error})


def test_write_message():
    writer = SimpleSingerWriter()
    message = RecordMessage(
        stream="test",
        record={"id": 1, "name": "test"},
    )
    with redirect_stdout(io.StringIO()) as out:
        writer.write_message(message)

    assert out.getvalue() == (
        '{"type":"RECORD","stream":"test","record":{"id":1,"name":"test"}}\n'
    )
