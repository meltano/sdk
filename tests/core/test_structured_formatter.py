"""Tests for StructuredFormatter functionality."""

from __future__ import annotations

import json
import logging
import sys
import typing as t
from io import StringIO

import pytest

from singer_sdk import metrics
from singer_sdk.logging import ConsoleFormatter, StructuredFormatter


class LoggerFactory(t.Protocol):
    def __call__(
        self,
        name: str,
        level: int = logging.INFO,
        formatter: logging.Formatter | None = None,
    ) -> tuple[logging.Logger, StringIO]: ...


class TestStructuredFormatter:
    """Test the StructuredFormatter class."""

    @pytest.fixture
    def point(self) -> metrics.Point:
        return metrics.Point(
            metric_type="counter",
            metric=metrics.Metric.RECORD_COUNT,
            value=150,
            tags={"stream": "users", "tap": "tap-postgres"},
        )

    @pytest.fixture
    def make_logger(self):
        """Factory fixture: creates a configured logger and cleans up after the test."""
        created: list[tuple[logging.Logger, logging.Handler, StringIO]] = []

        def _factory(
            name: str,
            level: int = logging.INFO,
            formatter: logging.Formatter | None = None,
        ) -> tuple[logging.Logger, StringIO]:
            logger = logging.getLogger(name)
            logger.setLevel(level)
            stream = StringIO()
            handler = logging.StreamHandler(stream)
            handler.setFormatter(
                formatter if formatter is not None else StructuredFormatter()
            )
            logger.addHandler(handler)
            created.append((logger, handler, stream))
            return logger, stream

        yield _factory

        for logger, handler, stream in created:
            logger.removeHandler(handler)
            stream.close()

    def test_structured_formatter_includes_extra_fields(
        self,
        make_logger: LoggerFactory,
    ):
        """Test that StructuredFormatter includes extra fields in output."""
        logger, log_stream = make_logger("test_logger")

        extra_fields = {
            "app_name": "tap-test",
            "stream_name": "users",
            "record_count": 42,
            "custom_field": "test_value",
        }
        logger.info("Test message with extras", extra=extra_fields)

        log_data = json.loads(log_stream.getvalue())

        assert log_data["app_name"] == "tap-test"
        assert log_data["stream_name"] == "users"
        assert log_data["message"] == "Test message with extras"
        assert log_data["extra"] == {
            "record_count": 42,
            "custom_field": "test_value",
        }

    def test_structured_formatter_with_defaults(self, make_logger: LoggerFactory):
        """Test that StructuredFormatter includes default fields."""
        defaults = {"version": "1.0.0"}
        logger, log_stream = make_logger(
            "test_logger_defaults",
            formatter=StructuredFormatter(defaults=defaults),
        )

        logger.info("Test message", extra={"stream_name": "users"})

        log_data = json.loads(log_stream.getvalue())

        assert log_data["app_name"] == "singer-sdk"
        assert log_data["stream_name"] == "users"
        assert log_data["message"] == "Test message"
        assert log_data["extra"] == {"version": "1.0.0"}

    def test_structured_formatter_without_extra_fields(
        self,
        make_logger: LoggerFactory,
    ):
        """Test that StructuredFormatter works without extra fields."""
        logger, log_stream = make_logger("test_logger_no_extras")

        logger.info("Simple test message")

        log_data = json.loads(log_stream.getvalue())

        assert log_data["message"] == "Simple test message"
        assert log_data["level"] == "info"
        assert "logger_name" in log_data
        assert "ts" in log_data

    def test_structured_formatter_with_exception(self, make_logger: LoggerFactory):
        """Test that StructuredFormatter handles exceptions properly."""
        logger, log_stream = make_logger("test_logger_exception", level=logging.ERROR)

        try:
            msg = "Test exception"
            raise ValueError(msg)  # noqa: TRY301
        except ValueError:
            logger.exception("Error occurred", extra={"error_code": 500})

        log_data = json.loads(log_stream.getvalue())

        assert log_data["message"] == "Error occurred"
        assert log_data["extra"] == {"error_code": 500}
        assert "exception" in log_data
        assert isinstance(log_data["exception"], dict)
        assert log_data["exception"]["type"] == "ValueError"
        assert log_data["exception"]["module"] == "builtins"
        assert log_data["exception"]["message"] == "Test exception"
        assert "traceback" in log_data["exception"]
        assert "notes" not in log_data["exception"]
        assert isinstance(log_data["exception"]["traceback"], list)
        assert len(log_data["exception"]["traceback"]) > 0
        frame = log_data["exception"]["traceback"][0]
        assert "filename" in frame
        assert "function" in frame
        assert "lineno" in frame
        assert "raise ValueError(msg)" in frame["line"]

    def test_structured_formatter_with_chained_exception(
        self,
        make_logger: LoggerFactory,
    ):
        """Test that StructuredFormatter handles exception chaining properly."""
        logger, log_stream = make_logger(
            "test_logger_chained_exception",
            level=logging.ERROR,
        )

        try:  # noqa: PLW0717
            try:
                msg = "Original error"
                raise ValueError(msg)  # noqa: TRY301
            except ValueError as e:
                msg = "Chained error"
                raise RuntimeError(msg) from e
        except RuntimeError:
            logger.exception("Chained error occurred")

        log_data = json.loads(log_stream.getvalue())

        assert log_data["message"] == "Chained error occurred"
        assert "exception" in log_data
        assert isinstance(log_data["exception"], dict)
        assert log_data["exception"]["type"] == "RuntimeError"
        assert log_data["exception"]["module"] == "builtins"
        assert log_data["exception"]["message"] == "Chained error"

        assert "cause" in log_data["exception"]
        assert isinstance(log_data["exception"]["cause"], dict)
        assert log_data["exception"]["cause"]["type"] == "ValueError"
        assert log_data["exception"]["cause"]["module"] == "builtins"
        assert log_data["exception"]["cause"]["message"] == "Original error"

    def test_structured_formatter_with_context_exception(
        self,
        make_logger: LoggerFactory,
    ):
        """Test that StructuredFormatter handles context exception properly."""
        logger, log_stream = make_logger(
            "test_logger_context_exception",
            level=logging.ERROR,
        )

        try:  # noqa: PLW0717
            try:
                msg = "Original error"
                raise ValueError(msg)  # noqa: TRY301
            except ValueError:
                msg = "Context error"
                raise RuntimeError(msg) from None
        except RuntimeError:
            logger.exception("Context error occurred")

        log_data = json.loads(log_stream.getvalue())

        assert log_data["message"] == "Context error occurred"
        assert "exception" in log_data
        assert isinstance(log_data["exception"], dict)
        assert log_data["exception"]["type"] == "RuntimeError"
        assert log_data["exception"]["module"] == "builtins"
        assert log_data["exception"]["message"] == "Context error"

        assert "context" in log_data["exception"]
        assert isinstance(log_data["exception"]["context"], dict)
        assert log_data["exception"]["context"]["type"] == "ValueError"
        assert log_data["exception"]["context"]["module"] == "builtins"
        assert log_data["exception"]["context"]["message"] == "Original error"

    def test_structured_formatter_without_exception_notes(
        self,
        make_logger: LoggerFactory,
    ):
        """Test that exceptions without notes omit the `notes` key."""
        logger, log_stream = make_logger(
            "test_logger_no_exception_notes",
            level=logging.ERROR,
        )

        try:
            msg = "Test exception"
            raise ValueError(msg)  # noqa: TRY301
        except ValueError:
            logger.exception("Error occurred")

        log_data = json.loads(log_stream.getvalue())

        assert log_data["message"] == "Error occurred"
        assert "exception" in log_data
        assert "notes" not in log_data["exception"]

    @pytest.mark.skipif(
        sys.version_info < (3, 11),
        reason="Exception notes are Python 3.11+",
    )
    def test_structured_formatter_with_exception_notes(
        self,
        make_logger: LoggerFactory,
    ):
        """Test that StructuredFormatter handles exception notes properly."""
        logger, log_stream = make_logger(
            "test_logger_exception_notes",
            level=logging.ERROR,
        )

        try:
            msg = "Test exception"
            exc = ValueError(msg)
            exc.add_note("Info")  # type: ignore[attr-defined]
            exc.add_note("Moar info")  # type: ignore[attr-defined]
            raise exc
        except ValueError:
            logger.exception("Error occurred")

        log_data = json.loads(log_stream.getvalue())

        assert log_data["message"] == "Error occurred"
        assert log_data["exception"]["notes"] == ["Info", "Moar info"]

    def test_structured_formatter_json_serialization_fallback(
        self,
        make_logger: LoggerFactory,
    ):
        """Test that StructuredFormatter handles non-serializable objects."""
        logger, log_stream = make_logger("test_logger_fallback")

        class NonSerializable:
            def __init__(self):
                self.data = "test"

            def __str__(self):
                return self.data

        logger.info("Test message", extra={"object": NonSerializable()})

        log_output = json.loads(log_stream.getvalue())

        assert isinstance(log_output, dict)
        assert log_output["message"] == "Test message"
        assert log_output["extra"] == {"object": "test"}

    def test_structured_formatter_with_args(self, make_logger: LoggerFactory):
        """Test that StructuredFormatter handles args properly."""
        logger, log_stream = make_logger("test_logger_args")

        logger.info("Test message with %s and %s", "arg1", "arg2")

        log_data = json.loads(log_stream.getvalue())

        assert log_data["message"] == "Test message with arg1 and arg2"

    def test_structured_formatter_with_metric_logs(
        self, make_logger, point: metrics.Point
    ):
        """Test that StructuredFormatter handles METRIC logs correctly."""
        logger, log_stream = make_logger("test_logger_metrics")

        metrics.log(logger, point)

        log_data = json.loads(log_stream.getvalue())

        assert log_data["message"] == "METRIC"
        assert "metric_info" in log_data
        assert log_data["metric_info"] == {
            "type": "counter",
            "metric": "record_count",
            "value": 150,
            "tags": {"stream": "users", "tap": "tap-postgres"},
        }

    def test_structured_formatter_vs_console_formatter_metric_handling(
        self,
        make_logger,
        point: metrics.Point,
    ):
        """Compare how StructuredFormatter and ConsoleFormatter handle METRIC logs."""
        structured_logger, structured_stream = make_logger("test_structured_metrics")
        console_logger, console_stream = make_logger(
            "test_console_metrics",
            formatter=ConsoleFormatter(),
        )

        metrics.log(structured_logger, point)
        metrics.log(console_logger, point)

        structured_data = json.loads(structured_stream.getvalue())
        console_output = console_stream.getvalue()

        assert structured_data["message"] == "METRIC"
        assert "metric_info" in structured_data
        assert structured_data["metric_info"] == {
            "type": "counter",
            "metric": "record_count",
            "value": 150,
            "tags": {"stream": "users", "tap": "tap-postgres"},
        }

        assert "METRIC" in console_output
        assert "record_count" in console_output
        assert "150" in console_output

    def test_structured_formatter_format_method_with_metric_record(
        self,
        point: metrics.Point,
    ):
        """Test StructuredFormatter.format() method directly with METRIC LogRecord."""
        formatter = StructuredFormatter()

        record = logging.LogRecord(
            name="tap_postgres",
            level=logging.INFO,
            pathname="/path/to/tap.py",
            lineno=42,
            msg="METRIC: %s",
            args=(point,),
            exc_info=None,
        )
        record.app_name = "tap-postgres"
        record.stream_name = "users"

        log_data = json.loads(formatter.format(record))

        assert log_data["message"] == "METRIC"
        assert "metric_info" in log_data
        assert log_data["app_name"] == "tap-postgres"
        assert log_data["stream_name"] == "users"
        assert log_data["logger_name"] == "tap_postgres"
        assert log_data["level"] == "info"
        assert log_data["metric_info"] == {
            "type": "counter",
            "metric": "record_count",
            "value": 150,
            "tags": {
                "stream": "users",
                "tap": "tap-postgres",
            },
        }

    def test_structured_formatter_getmessage_exception_fallback(self):
        """Test that StructuredFormatter handles getMessage() exceptions properly."""
        formatter = StructuredFormatter()

        record = logging.LogRecord(
            name="test_logger",
            level=logging.INFO,
            pathname="/path/to/test.py",
            lineno=123,
            msg="Test message with %s and %d args",
            args=(
                "string_arg",
                "invalid_int_arg",  # This will cause TypeError in getMessage()
            ),
            exc_info=None,
        )

        log_data = json.loads(formatter.format(record))

        assert log_data["message"] == "Test message with %s and %d args"
        assert log_data["logger_name"] == "test_logger"
        assert log_data["level"] == "info"

    def test_structured_formatter_getmessage_value_error_fallback(self):
        """Test that StructuredFormatter handles getMessage() ValueError properly."""
        formatter = StructuredFormatter()

        record = logging.LogRecord(
            name="test_logger",
            level=logging.INFO,
            pathname="/path/to/test.py",
            lineno=456,
            msg="Test message with {invalid_format",  # Invalid format string
            args=("arg1",),
            exc_info=None,
        )

        log_data = json.loads(formatter.format(record))

        assert log_data["message"] == "Test message with {invalid_format"
        assert log_data["logger_name"] == "test_logger"
        assert log_data["level"] == "info"
