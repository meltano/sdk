from __future__ import annotations

import io
from contextlib import redirect_stdout
from datetime import datetime

import pytest
from pytz import UTC, timezone

import singer_sdk._singerlib as singer
from singer_sdk._singerlib.messages import format_message


def test_exclude_null_dict():
    pairs = [("a", 1), ("b", None), ("c", 3)]
    assert singer.exclude_null_dict(pairs) == {"a": 1, "c": 3}


def test_format_message():
    message = singer.RecordMessage(
        stream="test",
        record={"id": 1, "name": "test"},
    )
    assert format_message(message) == (
        '{"type":"RECORD","stream":"test","record":{"id":1,"name":"test"}}\n'
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


def test_record_message_naive_time_extracted():
    """Check that record message' time_extracted must be timezone-aware."""
    with pytest.raises(ValueError, match="must be either None or an aware datetime"):
        singer.RecordMessage(
            stream="test",
            record={"id": 1, "name": "test"},
            time_extracted=datetime(2021, 1, 1),  # noqa: DTZ001
        )


def test_record_message_time_extracted_to_utc():
    """Check that record message's time_extracted is converted to UTC."""
    naive = datetime(2021, 1, 1, 12)  # noqa: DTZ001
    nairobi = timezone("Africa/Nairobi")

    record = singer.RecordMessage(
        stream="test",
        record={"id": 1, "name": "test"},
        time_extracted=nairobi.localize(naive),
    )
    assert record.time_extracted == datetime(2021, 1, 1, 9, tzinfo=UTC)


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


def test_bookmark_properties_not_string_or_list():
    """Check that schema message's bookmark_properties must be a string or list."""
    with pytest.raises(ValueError, match="must be a string or list"):
        singer.SchemaMessage(
            stream="test",
            schema={"type": "object", "properties": {"id": {"type": "integer"}}},
            bookmark_properties=1,
        )


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
