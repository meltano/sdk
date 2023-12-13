"""Test the custom format validator."""

from __future__ import annotations

import typing as t

import pytest
from typing_extensions import override

from singer_sdk.sinks.core import FastJSONSchemaValidator, InvalidRecord, Sink
from singer_sdk.target_base import Target

if t.TYPE_CHECKING:
    from singer_sdk.sinks.core import BaseJSONSchemaValidator


@pytest.fixture
def test_schema():
    """Return a test schema."""

    return {
        "type": "object",
        "properties": {
            "datetime_col": {"type": "string", "format": "date-time"},
        },
    }


@pytest.fixture
def target():
    """Return a target object."""

    class CustomTarget(Target):
        name = "test_target"

    return CustomTarget()


@pytest.fixture
def default_sink(target, test_schema):
    """Return a default sink object."""

    class CustomSink(Sink):
        """Custom sink class."""

        @override
        def process_batch(self, context: dict) -> None:
            pass

        @override
        def process_record(self, record: dict, context: dict) -> None:
            pass

    return CustomSink(target, "test_stream", test_schema, None)


@pytest.fixture
def draft7_sink(target, test_schema):
    """Return a sink object with Draft7 checks enabled."""

    class CustomSink(Sink):
        """Custom sink class."""

        validate_field_string_format = True

        @override
        def process_batch(self, context: dict) -> None:
            pass

        @override
        def process_record(self, record: dict, context: dict) -> None:
            pass

    return CustomSink(target, "test_stream", test_schema, None)


@pytest.fixture
def custom_sink(target, test_schema):
    """Return a sink object with Draft7 checks enabled and a custom datetime check."""

    class CustomSink(Sink):
        """Custom sink class."""

        validate_field_string_format = True

        @override
        def get_validator(self) -> BaseJSONSchemaValidator | None:
            return FastJSONSchemaValidator(
                self.schema,
                validate_formats=self.validate_field_string_format,
                format_validators={
                    "date-time": (
                        r"^(-?(?:[1-9][0-9]*)?[0-9]{4})-(1[0-2]|0[1-9])-(3[01]|0[1-9]|[12][0-9])"
                        r"T(2[0-3]|[01][0-9]):([0-5][0-9]):([0-5][0-9])(\.[0-9]+)?"
                        r"(Z|[+-](?:2[0-3]|[01][0-9]):[0-5][0-9])?$"
                    ),
                },
            )

        @override
        def process_batch(self, context: dict) -> None:
            pass

        @override
        def process_record(self, record: dict, context: dict) -> None:
            pass

    return CustomSink(target, "test_stream", test_schema, None)


@pytest.fixture
def default_checker(default_sink: Sink):
    """Return a default format checker."""
    return default_sink._validator


@pytest.fixture
def draft7_checker(draft7_sink: Sink):
    """Return a custom 'date-time' format checker."""
    return draft7_sink._validator


@pytest.fixture
def custom_checker(custom_sink: Sink):
    """Return a custom 'date-time' format checker."""
    return custom_sink._validator


@pytest.mark.parametrize(
    "col,value",
    [
        pytest.param(
            "datetime_col",
            "2020-01-01T20:25:00.0Z",
        ),
        pytest.param(
            "datetime_col",
            "2020-01-01T20:25:00.0+00:00",
        ),
        pytest.param(
            "datetime_col",
            "2020-01-01T20:25:00Z",
        ),
        pytest.param(
            "datetime_col",
            "2020-01-01T20:25:00.0",
        ),
        pytest.param(
            "datetime_col",
            "no way to fail",
        ),
    ],
)
def test_default_date_time_check(
    default_checker: BaseJSONSchemaValidator, col: str, value: str
):
    """Test the custom format checker with a date-time checker."""
    default_checker.validate({col: value})


@pytest.mark.parametrize(
    "col,value",
    [
        pytest.param(
            "datetime_col",
            "2020-01-01T20:25:00.0Z",
        ),
        pytest.param(
            "datetime_col",
            "2020-01-01T20:25:00.0+00:00",
        ),
        pytest.param(
            "datetime_col",
            "2020-01-01T20:25:00Z",
        ),
        pytest.param(
            "datetime_col",
            "2020-01-01T20:25:00.0",
            marks=[
                pytest.mark.xfail(
                    raises=InvalidRecord,
                    reason="Missing offset",
                )
            ],
        ),
        pytest.param(
            "datetime_col",
            "no way to fail",
            marks=[
                pytest.mark.xfail(
                    raises=InvalidRecord,
                    reason="String of text",
                )
            ],
        ),
    ],
)
def test_draft7_date_time_check(
    draft7_checker: BaseJSONSchemaValidator, col: str, value: str
):
    """Test the custom format checker with a date-time checker."""
    draft7_checker.validate({col: value})


@pytest.mark.parametrize(
    "col,value",
    [
        pytest.param(
            "datetime_col",
            "2020-01-01T20:25:00.0Z",
        ),
        pytest.param(
            "datetime_col",
            "2020-01-01T20:25:00.0+00:00",
        ),
        pytest.param(
            "datetime_col",
            "2020-01-01T20:25:00Z",
        ),
        pytest.param(
            "datetime_col",
            "2020-01-01T20:25:00.0",
        ),
        pytest.param(
            "datetime_col",
            "no way to fail",
            marks=[
                pytest.mark.xfail(
                    raises=InvalidRecord,
                    reason="String of text",
                )
            ],
        ),
    ],
)
def test_custom_date_time_check(
    custom_checker: BaseJSONSchemaValidator, col: str, value: str
):
    """Test the custom format checker with a date-time checker."""
    custom_checker.validate({col: value})
