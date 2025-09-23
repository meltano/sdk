"""Logging utilities for the Singer SDK."""

from __future__ import annotations

import json
import logging
import typing as t

DEFAULT_FORMAT = "{asctime:23s} | {levelname:8s} | {name:30s} | {message}"


class ConsoleFormatter(logging.Formatter):
    """Custom formatter for console logging."""

    def __init__(self) -> None:
        """Initialize the console formatter."""
        super().__init__(DEFAULT_FORMAT, style="{", defaults={"point": {}})

    def format(self, record: logging.LogRecord) -> str:
        """Format the log record as a console log message.

        Args:
            record: The log record to format.

        Returns:
            A formatted log message.
        """
        if record.msg == "METRIC" and (point := record.__dict__.get("point")):
            formatted_msg = f"{record.msg} {json.dumps(point, default=str)}"
            # Create a shallow copy of the record to avoid mutating the original
            record_copy = logging.LogRecord(
                name=record.name,
                level=record.levelno,
                pathname=record.pathname,
                lineno=record.lineno,
                msg=formatted_msg,
                args=record.args,
                exc_info=record.exc_info,
                func=record.funcName,
                sinfo=record.stack_info,
            )
            # Copy extra attributes
            record_copy.__dict__.update(record.__dict__)
            return super().format(record_copy)
        return super().format(record)


class StructuredFormatter(logging.Formatter):
    """Custom formatter for structured logging with contextual information."""

    def __init__(
        self,
        *,
        defaults: dict[str, t.Any] | None = None,
        **kwargs: t.Any,
    ) -> None:
        """Initialize the structured formatter.

        Args:
            defaults: Default field values to include in every log record.
            **kwargs: Keyword arguments for the parent formatter.
        """
        super().__init__(**kwargs)
        self._defaults = defaults

    def format(self, record: logging.LogRecord) -> str:
        """Format the log record as structured JSON.

        Args:
            record: The log record to format.

        Returns:
            Formatted log message as JSON string.
        """
        # Add default fields
        log_data = (
            self._defaults | record.__dict__
            if self._defaults is not None
            else record.__dict__.copy()
        )

        # Handle exception information
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        # Clean up fields that aren't JSON serializable or are redundant
        fields_to_remove = ["exc_info", "exc_text", "stack_info", "args"]
        for field in fields_to_remove:
            log_data.pop(field, None)

        # Format the main message
        if record.args:
            try:
                log_data["message"] = record.getMessage()
            except (TypeError, ValueError):
                log_data["message"] = record.msg
        else:
            log_data["message"] = record.msg

        # Remove the original msg field to avoid duplication
        log_data.pop("msg", None)

        return json.dumps(log_data, default=str, separators=(",", ":"))
