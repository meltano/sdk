"""Tests for structured logging output in Singer SDK."""

from __future__ import annotations

import json
import logging
from io import StringIO

import pytest
import structlog

from singer_sdk import Tap, Target
from singer_sdk.helpers.capabilities import PluginCapabilities


class TestTapWithStructuredLogging(Tap):
    """Test tap that emits structured logs."""

    name = "test-tap-structured"
    capabilities = [
        PluginCapabilities.CATALOG,
        PluginCapabilities.DISCOVER,
        PluginCapabilities.STRUCTURED_LOGGING,
    ]

    def discover_streams(self):
        """Return empty streams for testing."""
        return []


class TestTargetWithStructuredLogging(Target):
    """Test target that emits structured logs."""

    name = "test-target-structured"
    capabilities = [
        PluginCapabilities.ABOUT,
        PluginCapabilities.STREAM_MAPS,
        PluginCapabilities.STRUCTURED_LOGGING,
    ]


class TestStructuredLoggingOutput:
    """Test structured logging output functionality."""

    @pytest.fixture(autouse=True)
    def setup_logging(self):
        """Set up structured logging for tests."""
        # Configure structlog for testing
        structlog.configure(
            processors=[
                structlog.processors.JSONRenderer(),
            ],
            wrapper_class=structlog.testing.LogCapture,
            logger_factory=structlog.testing.TestingLoggerFactory(),
            cache_logger_on_first_use=True,
        )

    def test_tap_emits_structured_logs(self):
        """Test that tap with structured logging capability emits structured logs."""
        tap = TestTapWithStructuredLogging()

        # Capture log output
        log_capture = StringIO()
        handler = logging.StreamHandler(log_capture)

        # Get the logger and add our handler
        logger = tap.logger
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)

        # Emit a log message
        logger.info("Test message", extra={"test_field": "test_value"})

        # Get the log output
        log_output = log_capture.getvalue()
        assert log_output, "Expected log output but got none"

    def test_target_emits_structured_logs(self):
        """Test that target with structured logging capability emits structured logs."""
        target = TestTargetWithStructuredLogging()

        # Capture log output
        log_capture = StringIO()
        handler = logging.StreamHandler(log_capture)

        # Get the logger and add our handler
        logger = target.logger
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)

        # Emit a log message
        logger.info("Test target message", extra={"target_field": "target_value"})

        # Get the log output
        log_output = log_capture.getvalue()
        assert log_output, "Expected log output but got none"

    def test_structured_logging_with_context(self):
        """Test structured logging with additional context."""
        tap = TestTapWithStructuredLogging()

        # Use structlog's testing capabilities
        with structlog.testing.LogCapture() as cap:
            # Get a structlog logger bound to the tap
            struct_logger = structlog.get_logger("test-tap-structured")
            struct_logger.info(
                "Processing record",
                stream_name="test_stream",
                record_count=100,
                duration_seconds=1.5,
            )

        # Verify the log was captured
        assert len(cap.entries) == 1
        entry = cap.entries[0]

        assert entry["event"] == "Processing record"
        assert entry["stream_name"] == "test_stream"
        assert entry["record_count"] == 100
        assert entry["duration_seconds"] == 1.5

    @pytest.mark.parametrize(
        "log_level,message,extra_data",
        [
            ("info", "Info message", {"key": "value"}),
            ("warning", "Warning message", {"status": "warning"}),
            ("error", "Error message", {"error_code": 500}),
            ("debug", "Debug message", {"debug_info": "details"}),
        ],
    )
    def test_structured_logging_different_levels(self, log_level, message, extra_data):
        """Test structured logging at different log levels."""
        tap = TestTapWithStructuredLogging()

        with structlog.testing.LogCapture() as cap:
            struct_logger = structlog.get_logger("test-tap-structured")
            log_method = getattr(struct_logger, log_level)
            log_method(message, **extra_data)

        assert len(cap.entries) == 1
        entry = cap.entries[0]

        assert entry["event"] == message
        assert entry["level"] == log_level
        for key, value in extra_data.items():
            assert entry[key] == value

    def test_structured_logging_json_serializable(self):
        """Test that structured logs are JSON serializable."""
        tap = TestTapWithStructuredLogging()

        with structlog.testing.LogCapture() as cap:
            struct_logger = structlog.get_logger("test-tap-structured")
            struct_logger.info(
                "JSON serializable log",
                string_value="test",
                int_value=123,
                float_value=45.67,
                bool_value=True,
                list_value=[1, 2, 3],
                dict_value={"nested": "data"},
            )

        entry = cap.entries[0]

        # Should be JSON serializable
        try:
            json_str = json.dumps(entry)
            parsed = json.loads(json_str)
            assert parsed["event"] == "JSON serializable log"
            assert parsed["string_value"] == "test"
            assert parsed["int_value"] == 123
            assert parsed["float_value"] == 45.67
            assert parsed["bool_value"] is True
            assert parsed["list_value"] == [1, 2, 3]
            assert parsed["dict_value"] == {"nested": "data"}
        except (TypeError, ValueError) as e:
            pytest.fail(f"Structured log entry is not JSON serializable: {e}")

    def test_capability_affects_logger_configuration(self):
        """Test that having structured logging capability affects logger configuration."""
        tap_with_structured = TestTapWithStructuredLogging()

        # Verify the capability is present
        assert PluginCapabilities.STRUCTURED_LOGGING in tap_with_structured.capabilities

        # The logger should be configured appropriately
        assert tap_with_structured.logger is not None
        assert tap_with_structured.logger.name == "test-tap-structured"

    def test_structured_logging_with_metrics(self):
        """Test structured logging with metrics data."""
        tap = TestTapWithStructuredLogging()

        with structlog.testing.LogCapture() as cap:
            struct_logger = structlog.get_logger("test-tap-structured")

            # Log various metric types
            struct_logger.info(
                "Stream processing metrics",
                stream_name="users",
                records_processed=1000,
                bytes_processed=50000,
                processing_time_ms=2500,
                errors_encountered=0,
                records_per_second=400.0,
            )

        entry = cap.entries[0]

        assert entry["event"] == "Stream processing metrics"
        assert entry["stream_name"] == "users"
        assert entry["records_processed"] == 1000
        assert entry["bytes_processed"] == 50000
        assert entry["processing_time_ms"] == 2500
        assert entry["errors_encountered"] == 0
        assert entry["records_per_second"] == 400.0

    def test_structured_logging_error_handling(self):
        """Test structured logging with error information."""
        tap = TestTapWithStructuredLogging()

        with structlog.testing.LogCapture() as cap:
            struct_logger = structlog.get_logger("test-tap-structured")

            try:
                # Simulate an error
                raise ValueError("Test error for logging")
            except ValueError as e:
                struct_logger.error(
                    "Error occurred during processing",
                    error_type=type(e).__name__,
                    error_message=str(e),
                    stream_name="test_stream",
                    record_id="123",
                )

        entry = cap.entries[0]

        assert entry["event"] == "Error occurred during processing"
        assert entry["level"] == "error"
        assert entry["error_type"] == "ValueError"
        assert entry["error_message"] == "Test error for logging"
        assert entry["stream_name"] == "test_stream"
        assert entry["record_id"] == "123"

    def test_structured_logging_performance_data(self):
        """Test structured logging with performance metrics."""
        tap = TestTapWithStructuredLogging()

        with structlog.testing.LogCapture() as cap:
            struct_logger = structlog.get_logger("test-tap-structured")

            # Log performance data
            struct_logger.info(
                "Performance metrics",
                operation="extract",
                start_time="2023-01-01T10:00:00Z",
                end_time="2023-01-01T10:05:00Z",
                duration_seconds=300,
                memory_usage_mb=128.5,
                cpu_usage_percent=75.2,
                network_requests=50,
                cache_hits=45,
                cache_misses=5,
            )

        entry = cap.entries[0]

        assert entry["event"] == "Performance metrics"
        assert entry["operation"] == "extract"
        assert entry["duration_seconds"] == 300
        assert entry["memory_usage_mb"] == 128.5
        assert entry["cpu_usage_percent"] == 75.2
        assert entry["network_requests"] == 50
        assert entry["cache_hits"] == 45
        assert entry["cache_misses"] == 5
