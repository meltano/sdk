from __future__ import annotations

import datetime
import io
import json
from contextlib import redirect_stdout

import pytest
from pytz import timezone

import singer_sdk.singerlib as singer

UTC = datetime.timezone.utc


def test_exclude_null_dict():
    pairs = [("a", 1), ("b", None), ("c", 3)]
    assert singer.exclude_null_dict(pairs) == {"a": 1, "c": 3}


def test_format_message():
    message = singer.RecordMessage(
        stream="test",
        record={"id": 1, "name": "test"},
    )
    assert singer.format_message(message) == (
        '{"type":"RECORD","stream":"test","record":{"id":1,"name":"test"}}'
    )


def test_write_message():
    message = singer.RecordMessage(
        stream="test",
        record={"id": 1, "name": "test"},
    )
    with redirect_stdout(io.StringIO()) as out:
        singer.write_message(message)

    assert out.getvalue() == (
        '{"type":"RECORD","stream":"test","record":{"id":1,"name":"test"}}\n'
    )


def test_record_message():
    record = singer.RecordMessage(
        stream="test",
        record={"id": 1, "name": "test"},
    )
    assert record.stream == "test"
    assert record.record == {"id": 1, "name": "test"}
    assert record.to_dict() == {
        "type": "RECORD",
        "stream": "test",
        "record": {"id": 1, "name": "test"},
    }

    assert singer.RecordMessage.from_dict(record.to_dict()) == record


def test_record_message_parse_time_extracted():
    message_dic = {
        "type": "RECORD",
        "stream": "test",
        "record": {"id": 1, "name": "test"},
        "time_extracted": "2021-01-01T00:00:00Z",
    }
    record = singer.RecordMessage.from_dict(message_dic)
    assert record.type == "RECORD"
    assert record.stream == "test"
    assert record.record == {"id": 1, "name": "test"}
    assert record.time_extracted == datetime.datetime(2021, 1, 1, 0, 0, 0, tzinfo=UTC)


def test_record_message_naive_time_extracted():
    """Check that record message' time_extracted must be timezone-aware."""
    with pytest.raises(ValueError, match="must be either None or an aware datetime"):
        singer.RecordMessage(
            stream="test",
            record={"id": 1, "name": "test"},
            time_extracted=datetime.datetime(2021, 1, 1),  # noqa: DTZ001
        )


def test_record_message_time_extracted_to_utc():
    """Check that record message's time_extracted is converted to UTC."""
    naive = datetime.datetime(2021, 1, 1, 12)  # noqa: DTZ001
    nairobi = timezone("Africa/Nairobi")

    record = singer.RecordMessage(
        stream="test",
        record={"id": 1, "name": "test"},
        time_extracted=nairobi.localize(naive),
    )
    assert record.time_extracted == datetime.datetime(2021, 1, 1, 9, tzinfo=UTC)


def test_record_message_with_version():
    record = singer.RecordMessage(
        stream="test",
        record={"id": 1, "name": "test"},
        version=1614556800,
    )
    assert record.version == 1614556800
    assert record.to_dict() == {
        "type": "RECORD",
        "stream": "test",
        "record": {"id": 1, "name": "test"},
        "version": 1614556800,
    }


def test_schema_message():
    schema = singer.SchemaMessage(
        stream="test",
        schema={"type": "object", "properties": {"id": {"type": "integer"}}},
    )
    assert schema.stream == "test"
    assert schema.schema == {
        "type": "object",
        "properties": {"id": {"type": "integer"}},
    }
    assert schema.to_dict() == {
        "type": "SCHEMA",
        "stream": "test",
        "schema": {"type": "object", "properties": {"id": {"type": "integer"}}},
    }

    assert singer.SchemaMessage.from_dict(schema.to_dict()) == schema


def test_schema_messages_string_bookmark_properties():
    """Check that schema message's bookmark_properties can be a string."""
    schema = singer.SchemaMessage(
        stream="test",
        schema={"type": "object", "properties": {"id": {"type": "integer"}}},
        bookmark_properties="id",
    )
    assert schema.bookmark_properties == ["id"]


def test_state_message():
    state = singer.StateMessage(value={"bookmarks": {"test": {"id": 1}}})
    assert state.value == {"bookmarks": {"test": {"id": 1}}}
    assert state.to_dict() == {
        "type": "STATE",
        "value": {"bookmarks": {"test": {"id": 1}}},
    }

    assert singer.StateMessage.from_dict(state.to_dict()) == state


def test_activate_version_message():
    version = singer.ActivateVersionMessage(stream="test", version=1)
    assert version.stream == "test"
    assert version.version == 1
    assert version.to_dict() == {
        "type": "ACTIVATE_VERSION",
        "stream": "test",
        "version": 1,
    }

    assert singer.ActivateVersionMessage.from_dict(version.to_dict()) == version


def test_write_record_write_schema_write_state(capsys: pytest.CaptureFixture):
    singer.write_schema("s", {"properties": {"a": {"type": "integer"}}})
    singer.write_schema(
        "s",
        {"properties": {"a": {"type": "integer"}}},
        key_properties=["a"],
    )
    singer.write_schema(
        "s",
        {"properties": {"updated_at": {"type": "string", "format": "date-time"}}},
        bookmark_properties=["updated_at"],
    )

    singer.write_version("s", 123)

    singer.write_record("s", {"a": 1})
    singer.write_record("s", {"a": 2}, version=123)
    singer.write_record(
        "s",
        {"a": 3},
        time_extracted=datetime.datetime(2026, 1, 1, tzinfo=UTC),
    )

    singer.write_state({"bookmarks": {}})

    lines = [json.loads(line) for line in capsys.readouterr().out.splitlines()]

    assert lines[0:3] == [
        {
            "stream": "s",
            "type": "SCHEMA",
            "schema": {"properties": {"a": {"type": "integer"}}},
        },
        {
            "stream": "s",
            "type": "SCHEMA",
            "schema": {"properties": {"a": {"type": "integer"}}},
            "key_properties": ["a"],
        },
        {
            "stream": "s",
            "type": "SCHEMA",
            "schema": {
                "properties": {
                    "updated_at": {"type": "string", "format": "date-time"},
                }
            },
            "bookmark_properties": ["updated_at"],
        },
    ]

    assert lines[3] == {
        "stream": "s",
        "type": "ACTIVATE_VERSION",
        "version": 123,
    }

    assert lines[4:7] == [
        {
            "stream": "s",
            "type": "RECORD",
            "record": {"a": 1},
        },
        {
            "stream": "s",
            "type": "RECORD",
            "record": {"a": 2},
            "version": 123,
        },
        {
            "stream": "s",
            "type": "RECORD",
            "record": {"a": 3},
            "time_extracted": "2026-01-01T00:00:00+00:00",
        },
    ]

    assert lines[-1] == {
        "type": "STATE",
        "value": {"bookmarks": {}},
    }
