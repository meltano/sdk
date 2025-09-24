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
            record.msg = f"{record.msg} {json.dumps(point, default=str)}"
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
        data = (
            self._defaults | record.__dict__
            if self._defaults is not None
            else record.__dict__.copy()
        )

        # Clean up fields that aren't JSON serializable or are redundant
        fields_to_remove = [
            "args",
            "asctime",
            "created",
            "exc_info",
            "exc_text",
            "filename",
            "funcName",
            "levelname",
            "levelno",
            "lineno",
            "module",
            "msecs",
            "msg",  # Remove the original msg field to avoid duplication
            "name",
            "pathname",
            "process",
            "processName",
            "relativeCreated",
            "stack_info",
            "thread",
            "threadName",
            "taskName",
        ]
        for field in fields_to_remove:
            data.pop(field, None)

        log_data = {
            "level": record.levelname.lower(),
            "pid": record.process,
            "logger_name": record.name,
            "ts": record.created,
            "thread_name": record.threadName,
            "app_name": data.pop("app_name", "singer-sdk"),
            "stream_name": data.pop("stream_name", None),
        }

        # Handle metric information
        if point := data.pop("point", None):
            log_data["metric_info"] = point

        # Handle exception information
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        # Format the main message
        if record.args:
            try:
                log_data["message"] = record.getMessage()
            except (TypeError, ValueError):
                log_data["message"] = record.msg
        else:
            log_data["message"] = record.msg

        log_data["extra"] = data

        return json.dumps(log_data, default=str, separators=(",", ":"))
