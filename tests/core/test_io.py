"""Test IO operations."""

from __future__ import annotations

import decimal

import pytest

from singer_sdk.io_base import SingerReader


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
    "line,expected",
    [
        pytest.param(
            b'{"type": "RECORD", "stream": "users", "record": {"id": 1, "value": 1.23}}',  # noqa: E501
            {
                "type": "RECORD",
                "stream": "users",
                "record": {"id": 1, "value": decimal.Decimal("1.23")},
            },
            id="record",
        ),
    ],
)
def test_deserialize(line, expected):
    reader = DummyReader()
    assert reader.deserialize_json(line) == expected
