"""Test the custom format validator."""

from __future__ import annotations

import datetime

import pytest
from jsonschema import FormatChecker, FormatError

from singer_sdk.sinks.core import Sink
from singer_sdk.target_base import Target

ISOFORMAT = "%Y-%m-%dT%H:%M:%S.%f%z"
UTC = datetime.timezone.utc


@pytest.fixture(scope="module")
def target():
    """Return a target object."""

    class CustomTarget(Target):
        name = "test_target"

    return CustomTarget()


@pytest.fixture(scope="module")
def default_sink(target):
    """Return a sink object."""

    class CustomSink(Sink):
        """Custom sink class."""

        def process_batch(self, context: dict) -> None:
            pass

        def process_record(self, record: dict, context: dict) -> None:
            pass

    return CustomSink(target, "test_stream", {"properties": {}}, None)


@pytest.fixture(scope="module")
def default_checker(default_sink: Sink) -> FormatChecker:
    """Return a default format checker."""
    return default_sink.get_format_checker()


@pytest.fixture(scope="module")
def datetime_checker(default_sink: Sink) -> FormatChecker:
    """Return a custom 'date-time' format checker."""
    checker = default_sink.get_format_checker()

    @checker.checks("date-time", raises=ValueError)
    def check_time(instance: object) -> bool:
        try:
            datetime.datetime.strptime(instance, ISOFORMAT).replace(tzinfo=UTC)
        except ValueError:
            return False
        return True

    return checker


@pytest.mark.parametrize(
    "fmt,value",
    [
        pytest.param(
            "any string",
            "date-time",
        ),
        pytest.param(
            "any string",
            "date",
        ),
        pytest.param(
            "any string",
            "time",
        ),
    ],
)
def test_default_checks(default_checker: FormatChecker, value: str, fmt: str):
    """Test the Sink's default format checker."""
    default_checker.check(value, fmt)


@pytest.mark.parametrize(
    "fmt,value",
    [
        pytest.param(
            "date-time",
            "2020-01-01T20:25:00.0Z",
        ),
        pytest.param(
            "date-time",
            "2020-01-01T20:25:00.0+00:00",
        ),
        pytest.param(
            "date-time",
            "2020-01-01T20:25:00Z",
            marks=[pytest.mark.xfail(raises=FormatError, reason="Missing fraction")],
        ),
        pytest.param(
            "date-time",
            "2020-01-01T20:25:00.0",
            marks=[pytest.mark.xfail(raises=FormatError, reason="Missing offset")],
        ),
    ],
)
def test_custom_date_time_check(datetime_checker: FormatChecker, value: str, fmt: str):
    """Test the custom format checker with a date-time checker."""
    datetime_checker.check(value, fmt)
