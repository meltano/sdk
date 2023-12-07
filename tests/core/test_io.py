"""Test IO operations."""

from __future__ import annotations

import decimal
from contextlib import nullcontext

import msgspec
import pytest

from singer_sdk.io_base import SingerReader, dec_hook, enc_hook


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


def test_dec_hook():
    test_type = int
    test_obj = 1
    expected_value = "1"
    expceted_type = str
    returned = dec_hook(type=test_type, obj=test_obj)
    returned_type = type(returned)

    assert returned == expected_value
    assert returned_type == expceted_type


def test_enc_hook():
    test_obj = 1
    expected_value = "1"
    returned = enc_hook(obj=test_obj)

    assert returned == expected_value


@pytest.mark.parametrize(
    "line,expected,exception",
    [
        pytest.param(
            "not-valid-json",
            None,
            pytest.raises(msgspec.DecodeError),
            id="unparsable",
        ),
        pytest.param(
            b'{"type":"RECORD","stream":"users","record":{"id":1,"value":1.23}}',
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
