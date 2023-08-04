"""Test IO operations."""

from __future__ import annotations

import decimal
import typing as t
from contextlib import nullcontext

import msgspec
import pytest

from singer_sdk.io_base import SingerReader


class DummyReader(SingerReader):
    stream_structs: t.ClassVar[dict[str, msgspec.Struct]] = {
        "users": msgspec.defstruct("users", [("id", int), ("value", decimal.Decimal)]),
    }

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
            pytest.raises(msgspec.DecodeError),
            id="unparsable",
        ),
        pytest.param(
            b'{"type": "RECORD", "stream": "users", "record": {"id": 1, "value": 1.23}}',  # noqa: E501
            {
                "type": "RECORD",
                "stream": "users",
                "record": {"id": 1, "value": decimal.Decimal("1.23")},
                "version": None,
                "time_extracted": None,
            },
            nullcontext(),
            id="record",
        ),
    ],
)
def test_deserialize(line, expected, exception):
    reader = DummyReader()
    with exception:
        assert reader.deserialize_record(line) == expected
