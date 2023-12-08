"""Test IO operations."""

from __future__ import annotations

import decimal
import itertools
import json
import sys
import typing as t
from contextlib import nullcontext

import msgspec
import pytest

from singer_sdk._singerlib import RecordMessage
from singer_sdk.io_base import SingerReader, SingerWriter, dec_hook, enc_hook


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


# Benchmark Tests


@pytest.fixture
def bench_record():
    return {
        "stream": "users",
        "record": {
            "Id": 1,
            "created_at": "2021-01-01T00:08:00-07:00",
            "updated_at": "2022-01-02T00:09:00-07:00",
            "deleted_at": "2023-01-03T00:10:00-07:00",
            "value": 1.23,
            "RelatedtId": 32412,
            "TypeId": 1,
        },
        "time_extracted": "2023-01-01T11:00:00.00000-07:00",
    }


@pytest.fixture
def bench_record_message(bench_record):
    return RecordMessage.from_dict(bench_record)


@pytest.fixture
def bench_encoded_record(bench_record):
    return json.dumps(bench_record).encode()


def test_bench_format_message(benchmark, bench_record_message):
    """Run benchmark for Sink._validator method validate."""
    number_of_runs = 1000

    writer = SingerWriter()

    def run_format_message():
        for record in itertools.repeat(bench_record_message, number_of_runs):
            writer.format_message(record)

    benchmark(run_format_message)


def test_bench_deserialize_json(benchmark, bench_encoded_record):
    """Run benchmark for Sink._validator method validate."""
    number_of_runs = 1000

    reader = DummyReader()

    def run_deserialize_json():
        for record in itertools.repeat(bench_encoded_record, number_of_runs):
            reader.deserialize_json(record)

    benchmark(run_deserialize_json)
