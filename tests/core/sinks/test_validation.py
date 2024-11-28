from __future__ import annotations

import datetime
import itertools
import typing as t

import fastjsonschema
import pytest

from singer_sdk.exceptions import InvalidRecord
from singer_sdk.sinks.core import BaseJSONSchemaValidator, InvalidJSONSchema
from tests.conftest import BatchSinkMock, TargetMock


class FastJSONSchemaValidator(BaseJSONSchemaValidator):
    def __init__(self, schema: dict[str, t.Any]) -> None:
        super().__init__(schema)
        try:
            self.validator = fastjsonschema.compile(self.schema)
        except fastjsonschema.JsonSchemaDefinitionException as e:
            error_message = "Schema Validation Error"
            raise InvalidJSONSchema(error_message) from e

    def validate(self, record: dict):
        try:
            self.validator(record)
        except fastjsonschema.JsonSchemaValueException as e:
            error_message = f"Record Message Validation Error: {e.message}"
            raise InvalidRecord(error_message, record) from e


class FastJSONSchemaSink(BatchSinkMock):
    def get_validator(self) -> BaseJSONSchemaValidator | None:
        return FastJSONSchemaValidator(self.schema)


def test_validate_record():
    target = TargetMock()
    sink = BatchSinkMock(
        target,
        "users",
        {
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "created_at": {"type": "string", "format": "date-time"},
                "created_at_date": {"type": "string", "format": "date"},
                "created_at_time": {"type": "string", "format": "time"},
                "invalid_datetime": {"type": "string", "format": "date-time"},
            },
        },
        ["id"],
    )

    record = {
        "id": 1,
        "created_at": "2021-01-01T00:00:00+00:00",
        "created_at_date": "2021-01-01",
        "created_at_time": "00:01:00+00:00",
        "missing_datetime": "2021-01-01T00:00:00+00:00",
        "invalid_datetime": "not a datetime",
    }
    updated_record = sink._validate_and_parse(record)

    assert updated_record["created_at"] == datetime.datetime(
        2021,
        1,
        1,
        0,
        0,
        tzinfo=datetime.timezone.utc,
    )
    assert updated_record["created_at_date"] == datetime.date(
        2021,
        1,
        1,
    )
    assert updated_record["created_at_time"] == datetime.time(
        0,
        1,
        tzinfo=datetime.timezone.utc,
    )
    assert updated_record["missing_datetime"] == "2021-01-01T00:00:00+00:00"
    assert updated_record["invalid_datetime"] == "9999-12-31 23:59:59.999999"


def test_validate_fastjsonschema():
    target = TargetMock()
    sink = FastJSONSchemaSink(
        target,
        "users",
        {
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "created_at": {"type": "string", "format": "date-time"},
                "created_at_date": {"type": "string", "format": "date"},
                "created_at_time": {"type": "string", "format": "time"},
                "invalid_datetime": {"type": "string", "format": "date-time"},
            },
        },
        ["id"],
    )

    record = {
        "id": 1,
        "created_at": "2021-01-01T00:00:00+00:00",
        "created_at_date": "2021-01-01",
        "created_at_time": "00:01:00+00:00",
        "missing_datetime": "2021-01-01T00:00:00+00:00",
        "invalid_datetime": "not a datetime",
    }

    with pytest.raises(
        InvalidRecord,
        match=r"Record Message Validation Error",
    ) as exc_info:
        sink._validator.validate(record)

    assert isinstance(exc_info.value.__cause__, fastjsonschema.JsonSchemaValueException)


@pytest.fixture
def default_draft_sink_stop():
    """Return a sink object with the default draft checks enabled."""

    class CustomSink(BatchSinkMock):
        """Custom sink class."""

        validate_field_string_format = True

    return CustomSink(
        TargetMock(),
        "users",
        {
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "created_at": {"type": "string", "format": "date-time"},
                "created_at_date": {"type": "string", "format": "date"},
                "created_at_time": {"type": "string", "format": "time"},
                "invalid_datetime": {"type": "string", "format": "date-time"},
            },
        },
        ["id"],
    )


@pytest.fixture
def default_draft_sink_continue():
    """Return a sink object with the default draft checks enabled."""

    class CustomSink(BatchSinkMock):
        """Custom sink class."""

        validate_field_string_format = True
        fail_on_record_validation_exception = False

    return CustomSink(
        TargetMock(),
        "users",
        {
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "created_at": {"type": "string", "format": "date-time"},
                "created_at_date": {"type": "string", "format": "date"},
                "created_at_time": {"type": "string", "format": "time"},
                "invalid_datetime": {"type": "string", "format": "date-time"},
            },
        },
        ["id"],
    )


def test_validate_record_jsonschema_format_checking_enabled_stop_on_error(
    default_draft_sink_stop,
):
    sink: BatchSinkMock = default_draft_sink_stop

    record = {
        "id": 1,
        "created_at": "2021-01-01T00:00:00+00:00",
        "created_at_date": "2021-01-01",
        "created_at_time": "00:01:00+00:00",
        "missing_datetime": "2021-01-01T00:00:00+00:00",
        "invalid_datetime": "not a datetime",
    }
    with pytest.raises(
        InvalidRecord,
        match=r"Record Message Validation Error",
    ):
        sink._validate_and_parse(record)


def test_validate_record_jsonschema_format_checking_enabled_continue_on_error(
    capsys: pytest.CaptureFixture,
    default_draft_sink_continue,
):
    sink: BatchSinkMock = default_draft_sink_continue

    record = {
        "id": 1,
        "created_at": "2021-01-01T00:00:00+00:00",
        "created_at_date": "2021-01-01",
        "created_at_time": "00:01:00+00:00",
        "missing_datetime": "2021-01-01T00:00:00+00:00",
        "invalid_datetime": "not a datetime",
    }

    updated_record = sink._validate_and_parse(record)
    captured = capsys.readouterr()

    assert updated_record["created_at"] == datetime.datetime(
        2021,
        1,
        1,
        0,
        0,
        tzinfo=datetime.timezone.utc,
    )
    assert updated_record["created_at_date"] == datetime.date(
        2021,
        1,
        1,
    )
    assert updated_record["created_at_time"] == datetime.time(
        0,
        1,
        tzinfo=datetime.timezone.utc,
    )
    assert updated_record["missing_datetime"] == "2021-01-01T00:00:00+00:00"
    assert updated_record["invalid_datetime"] == "9999-12-31 23:59:59.999999"
    assert "Record Message Validation Error" in captured.err


@pytest.fixture
def bench_sink() -> BatchSinkMock:
    target = TargetMock()
    return BatchSinkMock(
        target,
        "users",
        {
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "created_at": {"type": "string", "format": "date-time"},
                "updated_at": {"type": "string", "format": "date-time"},
                "deleted_at": {"type": "string", "format": "date-time"},
            },
        },
        ["id"],
    )


@pytest.fixture
def bench_record():
    return {
        "id": 1,
        "created_at": "2021-01-01T00:08:00-07:00",
        "updated_at": "2022-01-02T00:09:00-07:00",
        "deleted_at": "2023-01-03T00:10:00.0000",
    }


def test_bench_parse_timestamps_in_record(benchmark, bench_sink, bench_record):
    """Run benchmark for Sink method _parse_timestamps_in_record."""
    number_of_runs = 1000

    sink: BatchSinkMock = bench_sink

    def run_parse_timestamps_in_record():
        for record in itertools.repeat(bench_record, number_of_runs):
            _ = sink._parse_timestamps_in_record(
                record.copy(), sink.schema, sink.datetime_error_treatment
            )

    benchmark(run_parse_timestamps_in_record)


def test_bench_validate_and_parse(benchmark, bench_sink, bench_record):
    """Run benchmark for Sink method _validate_and_parse."""
    number_of_runs = 1000

    sink: BatchSinkMock = bench_sink

    def run_validate_and_parse():
        for record in itertools.repeat(bench_record, number_of_runs):
            _ = sink._validate_and_parse(record.copy())

    benchmark(run_validate_and_parse)


def test_bench_validate_record_with_schema(benchmark, bench_sink, bench_record):
    """Run benchmark for Sink._validator method validate."""
    number_of_runs = 1000

    sink: BatchSinkMock = bench_sink

    def run_validate_record_with_schema():
        for record in itertools.repeat(bench_record, number_of_runs):
            sink._validator.validate(record)

    benchmark(run_validate_record_with_schema)
