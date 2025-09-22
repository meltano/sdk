"""Error handling utilities for structured logging."""

from __future__ import annotations

import enum
import sys
import typing as t
from dataclasses import dataclass

if t.TYPE_CHECKING:
    import logging


class ErrorCode(str, enum.Enum):
    """Standard error codes for Singer SDK plugins."""

    # Configuration errors
    CONFIG_VALIDATION_FAILED = "CONFIG_VALIDATION_FAILED"
    CONFIG_MISSING_REQUIRED = "CONFIG_MISSING_REQUIRED"

    # Connection errors
    CONNECTION_FAILED = "CONNECTION_FAILED"
    AUTHENTICATION_FAILED = "AUTHENTICATION_FAILED"

    # Discovery errors
    SCHEMA_DISCOVERY_FAILED = "SCHEMA_DISCOVERY_FAILED"

    # Stream errors
    STREAM_CONNECTION_FAILED = "STREAM_CONNECTION_FAILED"
    STREAM_SYNC_FAILED = "STREAM_SYNC_FAILED"
    RECORD_PROCESSING_FAILED = "RECORD_PROCESSING_FAILED"

    # Replication errors
    INVALID_REPLICATION_KEY = "INVALID_REPLICATION_KEY"
    STATE_PROCESSING_FAILED = "STATE_PROCESSING_FAILED"

    # API errors
    API_REQUEST_FAILED = "API_REQUEST_FAILED"
    API_RATE_LIMITED = "API_RATE_LIMITED"
    API_AUTHENTICATION_FAILED = "API_AUTHENTICATION_FAILED"

    # Target errors
    TARGET_LOAD_FAILED = "TARGET_LOAD_FAILED"
    TARGET_SCHEMA_MISSING = "TARGET_SCHEMA_MISSING"

    # Generic errors
    UNKNOWN_ERROR = "UNKNOWN_ERROR"


@dataclass
class StructuredError:
    """Structured error information for logging."""

    code: ErrorCode
    message: str
    details: dict[str, t.Any] | None = None
    exception: Exception | None = None

    def to_log_dict(self) -> dict[str, t.Any]:
        """Convert to dictionary for structured logging.

        Returns:
            Dictionary representation suitable for logging.
        """
        log_data: dict[str, t.Any] = {
            "error_code": self.code.value,
            "error_message": self.message,
        }

        if self.details is not None:
            log_data["error_details"] = self.details

        if self.exception:
            log_data["exception_type"] = self.exception.__class__.__name__
            log_data["exception_message"] = str(self.exception)

        return log_data


def log_and_exit(
    logger: logging.Logger,
    error: StructuredError,
    exit_code: int = 1,
) -> None:
    """Log a structured error and exit the process.

    This follows the pattern suggested in the requirements where the SDK should
    avoid raising exceptions for known errors and instead emit a single error-level
    log and exit.

    Args:
        logger: Logger instance to use for logging.
        error: Structured error information.
        exit_code: Exit code to use when terminating the process.
    """
    logger.error(
        "Plugin execution failed: %s",
        error.message,
        extra=error.to_log_dict(),
    )
    sys.exit(exit_code)


def log_structured_error(
    logger: logging.Logger,
    error: StructuredError,
) -> None:
    """Log a structured error without exiting.

    Args:
        logger: Logger instance to use for logging.
        error: Structured error information.
    """
    logger.error(
        "Error occurred: %s",
        error.message,
        extra=error.to_log_dict(),
    )


def create_error_from_exception(
    exc: Exception,
    code: ErrorCode | None = None,
    details: dict[str, t.Any] | None = None,
) -> StructuredError:
    """Create a structured error from an exception.

    Args:
        exc: The exception to convert.
        code: Error code to assign. If None, will attempt to infer.
        details: Additional details to include.

    Returns:
        StructuredError instance.
    """
    if code is None:
        code = _infer_error_code_from_exception(exc)

    return StructuredError(
        code=code,
        message=str(exc),
        details=details,
        exception=exc,
    )


def _infer_error_code_from_exception(exc: Exception) -> ErrorCode:
    """Infer error code from exception type.

    Args:
        exc: Exception to analyze.

    Returns:
        Inferred error code.
    """
    exc_name = exc.__class__.__name__.lower()

    # Map common exception name patterns to error codes
    error_mappings = [
        (["config", "validation"], ErrorCode.CONFIG_VALIDATION_FAILED),
        (["connection"], ErrorCode.CONNECTION_FAILED),
        (["auth"], ErrorCode.AUTHENTICATION_FAILED),
        (["discovery"], ErrorCode.SCHEMA_DISCOVERY_FAILED),
        (["replication"], ErrorCode.INVALID_REPLICATION_KEY),
        (["api", "request"], ErrorCode.API_REQUEST_FAILED),
    ]

    for keywords, error_code in error_mappings:
        if any(keyword in exc_name for keyword in keywords):
            return error_code

    return ErrorCode.UNKNOWN_ERROR
