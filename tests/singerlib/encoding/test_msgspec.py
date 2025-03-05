"""Test IO operations for msgspec Singer reader and writer."""

from __future__ import annotations

import decimal
import io
from contextlib import nullcontext, redirect_stdout
from textwrap import dedent

import pytest

from singer_sdk._singerlib import RecordMessage
from singer_sdk._singerlib.exceptions import InvalidInputLine
from singer_sdk.contrib.msgspec import (
    MsgSpecReader,
    MsgSpecWriter,
    dec_hook,
    enc_hook,
)

CALLBACKS = {
    "RECORD": lambda _: None,
    "SCHEMA": lambda _: None,
    "STATE": lambda _: None,
    "ACTIVATE_VERSION": lambda _: None,
    "BATCH": lambda _: None,
}


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
    reader = MsgSpecReader()
    with exception:
        assert reader.deserialize_json(line) == expected


def test_process_lines():
    reader = MsgSpecReader()
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
    reader = MsgSpecReader()
    input_lines = io.StringIO('{"type": "UNKNOWN"}\n')
    with pytest.raises(ValueError, match="Unknown message type"):
        reader.process_lines(input_lines, CALLBACKS)


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
