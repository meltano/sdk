"""Test IO operations."""

from __future__ import annotations

import decimal
import sys
import typing as t
from contextlib import nullcontext

import msgspec
import pytest

from singer_sdk.io_base import SingerReader, dec_hook, enc_hook


class DummyReader(SingerReader):
    returned_file_input: t.IO = None

    def _process_lines(self, file_input: t.IO) -> t.Counter[str]:
        self.returned_file_input = file_input

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
    "test_value,expected_value",
    [
        pytest.param(
            None,
            sys.stdin.buffer,
            id="file_input_is_none",
        ),
    ],
)
def test_listen_file_input(test_value, expected_value):
    reader = DummyReader()

    reader.listen(test_value)

    assert reader.returned_file_input is expected_value


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
