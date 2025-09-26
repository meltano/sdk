"""Tests for StructuredFormatter functionality."""

from __future__ import annotations

import json
import logging
from io import StringIO

import pytest

from singer_sdk import metrics
from singer_sdk.logging import ConsoleFormatter, StructuredFormatter


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

    def test_structured_formatter_includes_extra_fields(self):
        """Test that StructuredFormatter includes extra fields in output."""
        # Create a logger with StructuredFormatter
        logger = logging.getLogger("test_logger")
        logger.setLevel(logging.INFO)

        # Create a string stream to capture log output
        log_stream = StringIO()
        handler = logging.StreamHandler(log_stream)
        formatter = StructuredFormatter()
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        # Log a message with extra fields
        extra_fields = {
            "app_name": "tap-test",
            "stream_name": "users",
            "record_count": 42,
            "custom_field": "test_value",
        }
        logger.info("Test message with extras", extra=extra_fields)

        # Get the logged output
        log_output = log_stream.getvalue().strip()
        log_data = json.loads(log_output)

        # Verify that the extra fields are included
        assert log_data["app_name"] == "tap-test"
        assert log_data["stream_name"] == "users"
        assert log_data["message"] == "Test message with extras"
        assert log_data["extra"] == {
            "record_count": 42,
            "custom_field": "test_value",
        }

        # Clean up
        logger.removeHandler(handler)

    def test_structured_formatter_with_defaults(self):
        """Test that StructuredFormatter includes default fields."""
        # Create formatter with defaults
        defaults = {"version": "1.0.0"}
        formatter = StructuredFormatter(defaults=defaults)

        # Create a logger
        logger = logging.getLogger("test_logger_defaults")
        logger.setLevel(logging.INFO)

        log_stream = StringIO()
        handler = logging.StreamHandler(log_stream)
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        # Log a message with extra fields
        extra_fields = {"stream_name": "users"}
        logger.info("Test message", extra=extra_fields)

        # Get the logged output
        log_output = log_stream.getvalue().strip()
        log_data = json.loads(log_output)

        # Verify that both defaults and extra fields are included
        assert log_data["app_name"] == "singer-sdk"
        assert log_data["stream_name"] == "users"
        assert log_data["message"] == "Test message"
        assert log_data["extra"] == {
            "version": "1.0.0",
        }

        # Clean up
        logger.removeHandler(handler)

    def test_structured_formatter_without_extra_fields(self):
        """Test that StructuredFormatter works without extra fields."""
        logger = logging.getLogger("test_logger_no_extras")
        logger.setLevel(logging.INFO)

        log_stream = StringIO()
        handler = logging.StreamHandler(log_stream)
        formatter = StructuredFormatter()
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        # Log a simple message without extras
        logger.info("Simple test message")

        # Get the logged output
        log_output = log_stream.getvalue().strip()
        log_data = json.loads(log_output)

        # Verify basic structure
        assert log_data["message"] == "Simple test message"
        assert log_data["level"] == "info"
        assert "logger_name" in log_data
        assert "ts" in log_data

        # Clean up
        logger.removeHandler(handler)

    def test_structured_formatter_with_exception(self):
        """Test that StructuredFormatter handles exceptions properly."""
        logger = logging.getLogger("test_logger_exception")
        logger.setLevel(logging.ERROR)

        log_stream = StringIO()
        handler = logging.StreamHandler(log_stream)
        formatter = StructuredFormatter()
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        # Create an exception with proper exc_info
        try:
            msg = "Test exception"
            raise ValueError(msg)  # noqa: TRY301
        except ValueError:
            logger.exception("Error occurred", extra={"error_code": 500})

        # Get the logged output
        log_output = log_stream.getvalue().strip()
        log_data = json.loads(log_output)

        # Verify exception info and extra fields
        assert log_data["message"] == "Error occurred"
        assert log_data["extra"] == {
            "error_code": 500,
        }
        assert "exception" in log_data
        assert isinstance(log_data["exception"], dict)
        assert log_data["exception"]["type"] == "ValueError"
        assert log_data["exception"]["module"] == "builtins"
        assert log_data["exception"]["message"] == "Test exception"
        assert "traceback" in log_data["exception"]
        assert isinstance(log_data["exception"]["traceback"], list)
        assert len(log_data["exception"]["traceback"]) > 0
        # Verify traceback frame structure
        frame = log_data["exception"]["traceback"][0]
        assert "filename" in frame
        assert "function" in frame
        assert "lineno" in frame
        assert "raise ValueError(msg)" in frame["line"]

        # Clean up
        logger.removeHandler(handler)

    def test_structured_formatter_with_chained_exception(self):
        """Test that StructuredFormatter handles exception chaining properly."""
        logger = logging.getLogger("test_logger_chained_exception")
        logger.setLevel(logging.ERROR)

        log_stream = StringIO()
        handler = logging.StreamHandler(log_stream)
        formatter = StructuredFormatter()
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        # Create a chained exception
        try:
            try:
                msg = "Original error"
                raise ValueError(msg)  # noqa: TRY301
            except ValueError as e:
                msg = "Chained error"
                raise RuntimeError(msg) from e
        except RuntimeError:
            logger.exception("Chained error occurred")

        # Get the logged output
        log_output = log_stream.getvalue().strip()
        log_data = json.loads(log_output)

        # Verify main exception
        assert log_data["message"] == "Chained error occurred"
        assert "exception" in log_data
        assert isinstance(log_data["exception"], dict)
        assert log_data["exception"]["type"] == "RuntimeError"
        assert log_data["exception"]["module"] == "builtins"
        assert log_data["exception"]["message"] == "Chained error"

        # Verify chained exception (cause)
        assert "cause" in log_data["exception"]
        assert isinstance(log_data["exception"]["cause"], dict)
        assert log_data["exception"]["cause"]["type"] == "ValueError"
        assert log_data["exception"]["cause"]["module"] == "builtins"
        assert log_data["exception"]["cause"]["message"] == "Original error"

        # Clean up
        logger.removeHandler(handler)

    def test_structured_formatter_with_context_exception(self):
        """Test that StructuredFormatter handles context exception properly."""
        logger = logging.getLogger("test_logger_context_exception")
        logger.setLevel(logging.ERROR)

        log_stream = StringIO()
        handler = logging.StreamHandler(log_stream)
        formatter = StructuredFormatter()
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        # Create a context exception
        try:
            try:
                msg = "Original error"
                raise ValueError(msg)  # noqa: TRY301
            except ValueError:
                msg = "Context error"
                raise RuntimeError(msg) from None
        except RuntimeError:
            logger.exception("Context error occurred")

        # Get the logged output
        log_output = log_stream.getvalue().strip()
        log_data = json.loads(log_output)

        # Verify main exception
        assert log_data["message"] == "Context error occurred"
        assert "exception" in log_data
        assert isinstance(log_data["exception"], dict)
        assert log_data["exception"]["type"] == "RuntimeError"
        assert log_data["exception"]["module"] == "builtins"
        assert log_data["exception"]["message"] == "Context error"

        # Verify context exception
        assert "context" in log_data["exception"]
        assert isinstance(log_data["exception"]["context"], dict)
        assert log_data["exception"]["context"]["type"] == "ValueError"
        assert log_data["exception"]["context"]["module"] == "builtins"
        assert log_data["exception"]["context"]["message"] == "Original error"

    def test_structured_formatter_json_serialization_fallback(self):
        """Test that StructuredFormatter handles non-serializable objects."""
        logger = logging.getLogger("test_logger_fallback")
        logger.setLevel(logging.INFO)

        log_stream = StringIO()
        handler = logging.StreamHandler(log_stream)
        formatter = StructuredFormatter()
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        # Create a non-serializable object
        class NonSerializable:
            def __init__(self):
                self.data = "test"

            def __str__(self):
                return self.data

        non_serializable = NonSerializable()

        # Log with a non-serializable extra field
        logger.info("Test message", extra={"object": non_serializable})

        # Get the logged output
        log_output = json.loads(log_stream.getvalue().strip())

        # Should still produce valid output
        # (either JSON with str conversion or fallback)
        assert isinstance(log_output, dict)
        assert log_output["message"] == "Test message"
        assert log_output["extra"] == {
            "object": "test",
        }

        # Clean up
        logger.removeHandler(handler)

    def test_structured_formatter_with_args(self):
        """Test that StructuredFormatter handles args properly."""
        logger = logging.getLogger("test_logger_args")
        logger.setLevel(logging.INFO)

        log_stream = StringIO()
        handler = logging.StreamHandler(log_stream)
        formatter = StructuredFormatter()
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        # Log a message with args
        logger.info("Test message with %s and %s", "arg1", "arg2")

        # Get the logged output
        log_output = log_stream.getvalue().strip()
        log_data = json.loads(log_output)

        # Verify that the args are included
        assert log_data["message"] == "Test message with arg1 and arg2"

        # Clean up
        logger.removeHandler(handler)

    def test_structured_formatter_with_metric_logs(self, point: metrics.Point):
        """Test that StructuredFormatter handles METRIC logs correctly."""
        logger = logging.getLogger("test_logger_metrics")
        logger.setLevel(logging.INFO)

        log_stream = StringIO()
        handler = logging.StreamHandler(log_stream)
        formatter = StructuredFormatter()
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        # Log a METRIC message with point data (like ConsoleFormatter test)
        metrics.log(logger, point)

        # Get the logged output
        log_output = log_stream.getvalue().strip()
        log_data = json.loads(log_output)

        # Verify that the METRIC log is handled correctly
        assert log_data["message"] == "METRIC"
        assert "metric_info" in log_data
        assert log_data["metric_info"] == {
            "type": "counter",
            "metric": "record_count",
            "value": 150,
            "tags": {"stream": "users", "tap": "tap-postgres"},
        }

        # Clean up
        logger.removeHandler(handler)

    def test_structured_formatter_vs_console_formatter_metric_handling(
        self,
        point: metrics.Point,
    ):
        """Compare how StructuredFormatter and ConsoleFormatter handle METRIC logs."""
        # Test StructuredFormatter
        structured_logger = logging.getLogger("test_structured_metrics")
        structured_logger.setLevel(logging.INFO)
        structured_stream = StringIO()
        structured_handler = logging.StreamHandler(structured_stream)
        structured_formatter = StructuredFormatter()
        structured_handler.setFormatter(structured_formatter)
        structured_logger.addHandler(structured_handler)

        # Test ConsoleFormatter
        console_logger = logging.getLogger("test_console_metrics")
        console_logger.setLevel(logging.INFO)
        console_stream = StringIO()
        console_handler = logging.StreamHandler(console_stream)
        console_formatter = ConsoleFormatter()
        console_handler.setFormatter(console_formatter)
        console_logger.addHandler(console_handler)

        # Log with both formatters
        metrics.log(structured_logger, point)
        metrics.log(console_logger, point)

        # Get outputs
        structured_output = structured_stream.getvalue().strip()
        console_output = console_stream.getvalue().strip()

        # Parse structured output
        structured_data = json.loads(structured_output)

        # Verify structured output includes point as separate field
        assert structured_data["message"] == "METRIC"
        assert "metric_info" in structured_data
        assert structured_data["metric_info"] == {
            "type": "counter",
            "metric": "record_count",
            "value": 150,
            "tags": {"stream": "users", "tap": "tap-postgres"},
        }

        # Verify console output embeds point data in message
        assert "METRIC" in console_output
        assert "record_count" in console_output
        assert "150" in console_output

        # Clean up
        structured_logger.removeHandler(structured_handler)
        console_logger.removeHandler(console_handler)

    def test_structured_formatter_format_method_with_metric_record(
        self,
        point: metrics.Point,
    ):
        """Test StructuredFormatter.format() method directly with METRIC LogRecord."""
        formatter = StructuredFormatter()

        # Create a LogRecord that simulates what would be created for a METRIC log
        record = logging.LogRecord(
            name="tap_postgres",
            level=logging.INFO,
            pathname="/path/to/tap.py",
            lineno=42,
            msg="METRIC: %s",
            args=(point,),
            exc_info=None,
        )

        # Add extra fields that would be included in a METRIC log
        record.app_name = "tap-postgres"
        record.stream_name = "users"

        # Format the record directly
        formatted_output = formatter.format(record)

        # Parse and verify the JSON output
        log_data = json.loads(formatted_output)

        # Verify key fields are present and correct
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

        # Create a LogRecord with args that will cause getMessage() to fail
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

        # Format the record - this should trigger the getMessage() exception handling
        formatted_output = formatter.format(record)
        log_data = json.loads(formatted_output)

        # Verify that we get the original msg when getMessage() fails
        assert log_data["message"] == "Test message with %s and %d args"
        assert log_data["logger_name"] == "test_logger"
        assert log_data["level"] == "info"

    def test_structured_formatter_getmessage_value_error_fallback(self):
        """Test that StructuredFormatter handles getMessage() ValueError properly."""
        formatter = StructuredFormatter()

        # Create a LogRecord with args that will cause getMessage() to raise ValueError
        record = logging.LogRecord(
            name="test_logger",
            level=logging.INFO,
            pathname="/path/to/test.py",
            lineno=456,
            msg="Test message with {invalid_format",  # Invalid format string
            args=("arg1",),
            exc_info=None,
        )

        # Format the record - this should trigger the getMessage() exception handling
        formatted_output = formatter.format(record)
        log_data = json.loads(formatted_output)

        # Verify that we get the original msg when getMessage() fails
        assert log_data["message"] == "Test message with {invalid_format"
        assert log_data["logger_name"] == "test_logger"
        assert log_data["level"] == "info"
