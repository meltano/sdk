"""Logging utilities for the Singer SDK."""

from __future__ import annotations

import json
import linecache
import logging
import sys
import typing as t
from types import TracebackType

from singer_sdk import metrics

if sys.version_info >= (3, 11):
    from typing import Required  # noqa: ICN003
else:
    from typing_extensions import Required


class _FrameData(t.TypedDict):
    """Frame data."""

    filename: str
    function: str
    lineno: int
    line: str


class _ExceptionData(t.TypedDict, total=False):
    """Exception data."""

    type: Required[str]
    module: Required[str]
    message: Required[str]
    traceback: list[_FrameData]
    cause: _ExceptionData | None
    context: _ExceptionData | None


DEFAULT_FORMAT = "{asctime:23s} | {levelname:8s} | {name:30s} | {message}"

_SysExcInfoType: t.TypeAlias = (
    tuple[type[BaseException], BaseException, TracebackType | None]
    | tuple[None, None, None]
)


class ConsoleFormatter(logging.Formatter):
    """Custom formatter for console logging."""

    def __init__(self, **kwargs: t.Any) -> None:
        """Initialize the console formatter."""
        kwargs.setdefault("fmt", DEFAULT_FORMAT)
        kwargs.setdefault("style", "{")
        super().__init__(**kwargs)


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

    def _format_exception(
        self,
        exc_info: _SysExcInfoType,
    ) -> _ExceptionData | None:
        """Format exception information as a structured object.

        Args:
            exc_info: Exception info tuple from sys.exc_info().

        Returns:
            Structured exception data as a dictionary.
        """
        if exc_info[0] is None:  # pragma: no cover
            return None

        exc_type, exc_value, exc_traceback = exc_info

        # Extract basic exception info
        exception_data: _ExceptionData = {
            "type": exc_type.__name__,
            "module": exc_type.__module__,
            "message": str(exc_value),
        }

        # Add traceback frames
        tb: TracebackType | None
        if exc_traceback:  # pragma: no branch
            frames: list[_FrameData] = []
            tb = exc_traceback
            while tb is not None:
                frame_info: _FrameData = {
                    "filename": tb.tb_frame.f_code.co_filename,
                    "function": tb.tb_frame.f_code.co_name,
                    "lineno": tb.tb_lineno,
                    # TODO: Ensure newlines are stripped?
                    # TODO: Include one line above and one line below for context?
                    "line": linecache.getline(
                        tb.tb_frame.f_code.co_filename,
                        tb.tb_lineno,
                    ),
                }
                frames.append(frame_info)
                tb = tb.tb_next
            exception_data["traceback"] = frames

        # Handle exception chaining (__cause__ and __context__)
        if hasattr(exc_value, "__cause__") and exc_value.__cause__:
            exception_data["cause"] = self._format_exception(
                (
                    type(exc_value.__cause__),
                    exc_value.__cause__,
                    exc_value.__cause__.__traceback__,
                )
            )
        elif hasattr(exc_value, "__context__") and exc_value.__context__:
            exception_data["context"] = self._format_exception(
                (
                    type(exc_value.__context__),
                    exc_value.__context__,
                    exc_value.__context__.__traceback__,
                )
            )

        return exception_data

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

        # Handle exception information
        if record.exc_info and (exc_info := self._format_exception(record.exc_info)):
            log_data["exception"] = exc_info

        # Handle metric information
        if (
            record.msg == "METRIC: %s"
            and record.args
            and isinstance(record.args, tuple)
            and isinstance(record.args[0], metrics.Point)
        ):
            log_data["message"] = "METRIC"
            log_data["metric_info"] = record.args[0].to_dict()
        # Format the main message
        elif record.args:
            try:
                log_data["message"] = record.getMessage()
            except (TypeError, ValueError):
                log_data["message"] = record.msg
        else:
            log_data["message"] = record.msg

        log_data["extra"] = data

        return json.dumps(log_data, default=str, separators=(",", ":"))
