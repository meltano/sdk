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
        if defaults := self._defaults:
            log_data = defaults | record.__dict__
        else:
            log_data = record.__dict__.copy()

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

        try:
            return json.dumps(log_data, default=str, separators=(",", ":"))
        except (TypeError, ValueError):
            # Fallback to basic formatting if JSON serialization fails
            return super().format(record)


class ContextualLoggerAdapter(logging.LoggerAdapter):
    """Logger adapter that adds contextual information to log records."""

    def __init__(
        self,
        logger: logging.Logger,
        extra: dict[str, t.Any] | None = None,
    ) -> None:
        """Initialize the contextual logger adapter.

        Args:
            logger: The logger instance to wrap.
            extra: Additional context to include in log records.
        """
        super().__init__(logger, extra or {})

    def process(
        self, msg: t.Any, kwargs: dict[str, t.Any]
    ) -> tuple[t.Any, dict[str, t.Any]]:
        """Process the logging call.

        Args:
            msg: The log message.
            kwargs: Keyword arguments for the log call.

        Returns:
            Tuple of processed message and kwargs.
        """
        # Merge extra fields into the log record
        extra = kwargs.setdefault("extra", {})
        extra.update(self.extra)
        return msg, kwargs

    def set_context(self, **context: t.Any) -> None:
        """Update the context for this logger adapter.

        Args:
            **context: Context fields to set.
        """
        self.extra.update(context)

    def clear_context(self) -> None:
        """Clear all context from this logger adapter."""
        self.extra.clear()
