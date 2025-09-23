"""Tests for StructuredFormatter functionality."""

from __future__ import annotations

import json
import logging
from io import StringIO

from singer_sdk.logging import ConsoleFormatter, StructuredFormatter


class TestStructuredFormatter:
    """Test the StructuredFormatter class."""

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
            "plugin_name": "tap-test",
            "stream_name": "users",
            "record_count": 42,
            "custom_field": "test_value",
        }
        logger.info("Test message with extras", extra=extra_fields)

        # Get the logged output
        log_output = log_stream.getvalue().strip()
        log_data = json.loads(log_output)

        # Verify that the extra fields are included
        assert log_data["plugin_name"] == "tap-test"
        assert log_data["stream_name"] == "users"
        assert log_data["record_count"] == 42
        assert log_data["custom_field"] == "test_value"
        assert log_data["message"] == "Test message with extras"

        # Clean up
        logger.removeHandler(handler)

    def test_structured_formatter_with_defaults(self):
        """Test that StructuredFormatter includes default fields."""
        # Create formatter with defaults
        defaults = {"app_name": "singer-sdk", "version": "1.0.0"}
        formatter = StructuredFormatter(defaults=defaults)

        # Create a logger
        logger = logging.getLogger("test_logger_defaults")
        logger.setLevel(logging.INFO)

        log_stream = StringIO()
        handler = logging.StreamHandler(log_stream)
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        # Log a message with extra fields
        extra_fields = {"plugin_name": "tap-test", "stream_name": "users"}
        logger.info("Test message", extra=extra_fields)

        # Get the logged output
        log_output = log_stream.getvalue().strip()
        log_data = json.loads(log_output)

        # Verify that both defaults and extra fields are included
        assert log_data["app_name"] == "singer-sdk"
        assert log_data["version"] == "1.0.0"
        assert log_data["plugin_name"] == "tap-test"
        assert log_data["stream_name"] == "users"
        assert log_data["message"] == "Test message"

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
        assert log_data["levelname"] == "INFO"
        assert "name" in log_data
        assert "created" in log_data

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

        # Create an exception
        exc = ValueError("Test exception")
        logger.error("Error occurred", extra={"error_code": 500}, exc_info=exc)

        # Get the logged output
        log_output = log_stream.getvalue().strip()
        log_data = json.loads(log_output)

        # Verify exception info and extra fields
        assert log_data["message"] == "Error occurred"
        assert log_data["error_code"] == 500
        assert "exception" in log_data
        assert "ValueError" in log_data["exception"]

        # Clean up
        logger.removeHandler(handler)

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
        assert log_output["object"] == "test"

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

    def test_structured_formatter_with_metric_logs(self):
        """Test that StructuredFormatter handles METRIC logs correctly."""
        logger = logging.getLogger("test_logger_metrics")
        logger.setLevel(logging.INFO)

        log_stream = StringIO()
        handler = logging.StreamHandler(log_stream)
        formatter = StructuredFormatter()
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        # Log a METRIC message with point data (like ConsoleFormatter test)
        metric_point = {
            "metric_name": "records_processed",
            "value": 150,
            "tags": {"stream": "users", "tap": "tap-postgres"},
        }
        logger.info("METRIC", extra={"point": metric_point})

        # Get the logged output
        log_output = log_stream.getvalue().strip()
        log_data = json.loads(log_output)

        # Verify that the METRIC log is handled correctly
        assert log_data["message"] == "METRIC"
        assert "point" in log_data
        assert log_data["point"]["metric_name"] == "records_processed"
        assert log_data["point"]["value"] == 150
        assert log_data["point"]["tags"]["stream"] == "users"
        assert log_data["point"]["tags"]["tap"] == "tap-postgres"

        # Clean up
        logger.removeHandler(handler)

    def test_structured_formatter_vs_console_formatter_metric_handling(self):
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

        # Same metric data
        metric_point = {"metric_name": "records_processed", "value": 150}

        # Log with both formatters
        structured_logger.info("METRIC", extra={"point": metric_point})
        console_logger.info("METRIC", extra={"point": metric_point})

        # Get outputs
        structured_output = structured_stream.getvalue().strip()
        console_output = console_stream.getvalue().strip()

        # Parse structured output
        structured_data = json.loads(structured_output)

        # Verify structured output includes point as separate field
        assert structured_data["message"] == "METRIC"
        assert "point" in structured_data
        assert structured_data["point"]["metric_name"] == "records_processed"

        # Verify console output embeds point data in message
        assert "METRIC" in console_output
        assert "records_processed" in console_output
        assert "150" in console_output

        # Clean up
        structured_logger.removeHandler(structured_handler)
        console_logger.removeHandler(console_handler)

    def test_structured_formatter_format_method_with_metric_record(self):
        """Test StructuredFormatter.format() method directly with METRIC LogRecord."""
        formatter = StructuredFormatter()

        # Create a LogRecord that simulates what would be created for a METRIC log
        record = logging.LogRecord(
            name="tap_postgres",
            level=logging.INFO,
            pathname="/path/to/tap.py",
            lineno=42,
            msg="METRIC",
            args=(),
            exc_info=None,
        )

        # Add extra fields that would be included in a METRIC log
        record.point = {
            "metric_name": "records_processed",
            "value": 150,
            "tags": {"stream": "users", "tap": "tap-postgres"},
        }
        record.plugin_name = "tap-postgres"
        record.stream_name = "users"

        # Format the record directly
        formatted_output = formatter.format(record)

        # Parse and verify the JSON output
        log_data = json.loads(formatted_output)

        # Verify key fields are present and correct
        assert log_data["message"] == "METRIC"
        assert "point" in log_data
        assert isinstance(log_data["point"], dict)
        assert log_data["point"]["metric_name"] == "records_processed"
        assert log_data["point"]["value"] == 150
        assert log_data["point"]["tags"]["stream"] == "users"
        assert log_data["plugin_name"] == "tap-postgres"
        assert log_data["stream_name"] == "users"
        assert log_data["name"] == "tap_postgres"
        assert log_data["levelname"] == "INFO"

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
        assert log_data["name"] == "test_logger"
        assert log_data["levelname"] == "INFO"

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
        assert log_data["name"] == "test_logger"
        assert log_data["levelname"] == "INFO"
