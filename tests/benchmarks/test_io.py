"""Test IO operations."""

from __future__ import annotations

import itertools
import json
import os
import sys

import pytest

from singer_sdk.singerlib import RecordMessage, SchemaMessage, StateMessage

# IO Benchmarks


@pytest.fixture
def bench_record():
    return {
        "stream": "users",
        "type": "RECORD",
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
    return json.dumps(bench_record)


def test_bench_format_message(benchmark, bench_record_message: RecordMessage):
    """Run benchmark for Sink._validator method validate."""
    from singer_sdk.contrib.msgspec import MsgSpecWriter  # noqa: PLC0415

    number_of_runs = 1000

    writer = MsgSpecWriter()

    def run_format_message():
        for record in itertools.repeat(bench_record_message, number_of_runs):
            writer.format_message(record)

    benchmark(run_format_message)


def test_bench_deserialize_json(benchmark, bench_encoded_record: str):
    """Run benchmark for Sink._validator method validate."""
    from singer_sdk.contrib.msgspec import MsgSpecReader  # noqa: PLC0415

    number_of_runs = 1000
    reader = MsgSpecReader()

    def run_deserialize_json():
        for record in itertools.repeat(bench_encoded_record, number_of_runs):
            reader.deserialize_json(record)

    benchmark(run_deserialize_json)


@pytest.fixture
def bench_schema_message():
    return SchemaMessage(
        stream="users",
        schema={
            "type": "object",
            "properties": {
                "Id": {"type": "integer"},
                "created_at": {"type": "string"},
                "updated_at": {"type": "string"},
                "deleted_at": {"type": ["string", "null"]},
                "value": {"type": "number"},
                "RelatedtId": {"type": "integer"},
                "TypeId": {"type": "integer"},
            },
        },
        key_properties=["Id"],
    )


@pytest.fixture
def bench_state_message():
    return StateMessage(value={"bookmarks": {"users": {"Id": 1000}}})


def test_bench_write_record_messages_simple(
    benchmark,
    bench_record_message: RecordMessage,
):
    """Benchmark SimpleSingerWriter.write_message throughput for record messages.

    Redirects stdout to /dev/null so flush() makes real syscalls and is not a no-op.
    """
    from singer_sdk.singerlib.encoding.simple import SimpleSingerWriter  # noqa: PLC0415

    writer = SimpleSingerWriter()
    number_of_runs = 1000

    with open(os.devnull, "w", encoding="utf-8") as devnull:  # noqa: PTH123
        old_stdout = sys.stdout
        sys.stdout = devnull
        try:

            def run():
                for record in itertools.repeat(bench_record_message, number_of_runs):
                    writer.write_message(record)

            benchmark(run)
        finally:
            sys.stdout = old_stdout


def test_bench_write_record_messages_msgspec(
    benchmark,
    bench_record_message: RecordMessage,
):
    """Benchmark MsgSpecWriter.write_message throughput for record messages.

    Redirects stdout to /dev/null so flush() makes real syscalls and is not a no-op.
    """
    from singer_sdk.contrib.msgspec import MsgSpecWriter  # noqa: PLC0415

    writer = MsgSpecWriter()
    number_of_runs = 1000

    devnull_b = open(os.devnull, "wb")  # noqa: PTH123, SIM115

    class _DevNullStdout:
        buffer = devnull_b

        def write(self, data: str) -> int:
            """Mimic TextIOBase.write, discarding data like /dev/null."""
            return len(data)

        def flush(self) -> None:
            devnull_b.flush()

    old_stdout = sys.stdout
    sys.stdout = _DevNullStdout()  # type: ignore[assignment]
    try:

        def run():
            for record in itertools.repeat(bench_record_message, number_of_runs):
                writer.write_message(record)

        benchmark(run)
    finally:
        sys.stdout = old_stdout
        devnull_b.close()


def test_bench_write_mixed_messages_simple(
    benchmark,
    bench_record_message: RecordMessage,
    bench_schema_message: SchemaMessage,
    bench_state_message: StateMessage,
):
    """Benchmark SimpleSingerWriter: realistic schema→records→state sequence."""
    from singer_sdk.singerlib.encoding.simple import SimpleSingerWriter  # noqa: PLC0415

    writer = SimpleSingerWriter()
    records_per_state = 100

    with open(os.devnull, "w", encoding="utf-8") as devnull:  # noqa: PTH123
        old_stdout = sys.stdout
        sys.stdout = devnull
        try:

            def run():
                writer.write_message(bench_schema_message)
                for record in itertools.repeat(bench_record_message, records_per_state):
                    writer.write_message(record)
                writer.write_message(bench_state_message)

            benchmark(run)
        finally:
            sys.stdout = old_stdout
