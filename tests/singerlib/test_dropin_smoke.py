"""End-to-end smoke tests for the ``singer`` drop-in surface."""

from __future__ import annotations

import decimal
import json
import typing as t
from datetime import datetime, timezone

import singer
import singer.json
import singer.messages

if t.TYPE_CHECKING:
    import pytest


def test_write_message_stdout(capsys: pytest.CaptureFixture):
    singer.write_message(singer.RecordMessage(stream="s", record={"a": 1}))
    out = capsys.readouterr().out
    assert json.loads(out) == {"type": "RECORD", "stream": "s", "record": {"a": 1}}


def test_write_message_delegates_to_patched_messages_write_message(
    monkeypatch: pytest.MonkeyPatch,
):
    calls = []
    monkeypatch.setattr(singer.messages, "write_message", calls.append)
    msg = singer.RecordMessage(stream="s", record={"a": 1})
    singer.write_message(msg)
    assert calls == [msg]


def test_write_record_write_schema_write_state(capsys: pytest.CaptureFixture):
    singer.write_schema("s", {"properties": {"a": {"type": "integer"}}}, "a")
    singer.write_record("s", {"a": 1})
    singer.write_state({"bookmarks": {}})
    singer.write_version("s", 123)

    lines = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert [line["type"] for line in lines] == [
        "SCHEMA",
        "RECORD",
        "STATE",
        "ACTIVATE_VERSION",
    ]


def test_should_sync_field():
    assert singer.should_sync_field("automatic", False) is True
    assert singer.should_sync_field("unsupported", True) is False
    assert singer.should_sync_field("available", True) is True
    assert singer.should_sync_field("available", None, default=True) is True
    assert singer.should_sync_field("available", None) is False


def test_record_message_asdict_formats_time_extracted():
    time_extracted = datetime(2024, 1, 1, tzinfo=timezone.utc)
    msg = singer.RecordMessage(
        stream="s",
        record={"a": 1},
        time_extracted=time_extracted,
    )
    result = msg.asdict()
    assert result["time_extracted"] == "2024-01-01T00:00:00.000000Z"
    # `to_dict` (the SDK serialization path) keeps a datetime object.
    assert msg.to_dict()["time_extracted"] == time_extracted


def test_message_asdict_matches_to_dict_for_non_record_messages():
    msg = singer.StateMessage(value={"bookmarks": {}})
    assert msg.asdict() == msg.to_dict()


def test_decimal_precision_round_trip():
    value = singer.json.deserialize_json('{"multipleOf": 1e-38}')
    assert value["multipleOf"] == decimal.Decimal("1e-38")

    serialized = singer.json.serialize_json(value)
    round_tripped = singer.json.deserialize_json(serialized)
    assert round_tripped["multipleOf"] == decimal.Decimal("1e-38")
