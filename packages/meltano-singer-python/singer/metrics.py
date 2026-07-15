"""Singer metrics logging."""

from __future__ import annotations

import abc
import enum
import json
import logging
import os
import sys
import typing as t
from dataclasses import dataclass, field
from time import time

if sys.version_info >= (3, 12):
    from typing import override  # noqa: ICN003
else:
    from typing_extensions import override

if t.TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from types import TracebackType

    if sys.version_info >= (3, 11):
        from typing import Self  # noqa: ICN003
    else:
        from typing_extensions import Self

DEFAULT_LOG_INTERVAL = 60.0
METRICS_LOGGER_NAME = "singer_sdk.metrics"

_TVal = t.TypeVar("_TVal")


class Status(str, enum.Enum):
    """Constants for commonly used status values."""

    SUCCEEDED = "succeeded"
    FAILED = "failed"


class Tag(str, enum.Enum):
    """Constants for commonly used tags."""

    STREAM = "stream"
    CONTEXT = "context"
    ENDPOINT = "endpoint"
    JOB_TYPE = "job_type"
    HTTP_STATUS_CODE = "http_status_code"
    STATUS = "status"
    PID = "pid"


class Metric(str, enum.Enum):
    """Common metric types."""

    RECORD_COUNT = "record_count"
    BATCH_COUNT = "batch_count"
    HTTP_REQUEST_DURATION = "http_request_duration"
    HTTP_REQUEST_COUNT = "http_request_count"
    JOB_DURATION = "job_duration"
    SYNC_DURATION = "sync_duration"
    BATCH_PROCESSING_TIME = "batch_processing_time"


def _metric_name(metric: Metric | str) -> str:
    return metric.value if isinstance(metric, Metric) else metric


@dataclass(slots=True)
class Point(t.Generic[_TVal]):
    """An individual metric measurement."""

    metric_type: str
    metric: Metric | str
    value: _TVal
    tags: dict[str, t.Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, t.Any]:
        """Convert this measure to a dictionary.

        Returns:
            A dictionary.
        """
        return {
            "type": self.metric_type,
            "metric": _metric_name(self.metric),
            "value": self.value,
            "tags": self.tags,
        }

    @override
    def __str__(self) -> str:
        """Convert this measure to a string.

        Returns:
            A string.
        """
        return json.dumps(self.to_dict(), default=str, separators=(",", ":"))


class MetricExclusionFilter(logging.Filter):
    """A filter for excluding metrics from logging."""

    def __init__(
        self,
        *,
        metrics: Sequence[str | Metric] | None = None,
        types: Sequence[str] | None = None,
        tags: dict[str, t.Any] | None = None,
    ) -> None:
        """Initialize a metric filter.

        Args:
            metrics: A set of metrics to exclude.
            types: A set of metric types to exclude.
            tags: A dictionary of tags to exclude.
        """
        self.metrics = metrics or []
        self.types = types or []
        self.tags = tags or {}

    def _exclude_point(self, point: Point) -> bool:
        """Filter a point.

        Args:
            point: The point.

        A metric record is excluded if any of the following are true:
        - The metric name matches one in the metrics list
        - The metric type matches one in the types list
        - Any of the point's tags match the corresponding values in the tags dictionary

        Returns:
            True if the point should be excluded.
        """
        return (
            (_metric_name(point.metric) in self.metrics)
            or (point.metric_type in self.types)
            or any(point.tags.get(tag) == value for tag, value in self.tags.items())
        )

    @override
    def filter(self, record: logging.LogRecord) -> bool:
        """Filter a log record.

        Args:
            record: A log record.

        Returns:
            True if the record should be logged.
        """
        return not (
            record.args
            and isinstance(record.args, tuple)
            and (point := record.args[0])
            and isinstance(point, Point)
            and self._exclude_point(point)
        )


def log(logger: logging.Logger, point: Point) -> None:
    """Log a measurement.

    Args:
        logger: An logger instance.
        point: A measurement.
    """
    logger.info("METRIC: %s", point)


class Meter(abc.ABC):
    """Base class for all meters."""

    def __init__(self, metric: Metric | str, tags: dict | None = None) -> None:
        """Initialize a meter.

        Args:
            metric: The metric type.
            tags: Tags to add to the measurement.
        """
        self.metric = metric
        self.tags = tags or {}
        self.tags[Tag.PID] = os.getpid()
        self.logger = get_metrics_logger()

    @property
    def context(self) -> Mapping[str, t.Any] | None:
        """The context dictionary for this meter."""
        return self.tags.get(Tag.CONTEXT)

    @context.setter
    def context(self, value: Mapping[str, t.Any] | None) -> None:
        """Set the context for this meter.

        Args:
            value: A context dictionary.
        """
        self.with_context(value)

    def with_context(self, value: Mapping[str, t.Any] | None) -> None:
        """Set the context for this meter.

        Args:
            value: A context dictionary.
        """
        if value is None:
            self.tags.pop(Tag.CONTEXT, None)
        else:
            self.tags[Tag.CONTEXT] = value

    @abc.abstractmethod
    def __enter__(self) -> Meter:
        """Enter the meter context."""
        ...

    @abc.abstractmethod
    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Exit the meter context.

        Args:
            exc_type: The exception type.
            exc_val: The exception value.
            exc_tb: The exception traceback.
        """
        ...


class Counter(Meter):
    """A meter for counting things."""

    def __init__(
        self,
        metric: Metric | str,
        tags: dict | None = None,
        log_interval: float = DEFAULT_LOG_INTERVAL,
    ) -> None:
        """Initialize a counter.

        Args:
            metric: The metric type.
            tags: Tags to add to the measurement.
            log_interval: The interval at which to log the count.
        """
        super().__init__(metric, tags)
        self.value = 0
        self.log_interval = log_interval
        self.last_log_time = time()

    def exit(self) -> None:
        """Exit the counter context."""
        self._pop()

    @override
    def __enter__(self) -> Self:
        """Enter the counter context.

        Returns:
            The counter instance.
        """
        self.last_log_time = time()
        return self

    @override
    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Exit the counter context.

        Args:
            exc_type: The exception type.
            exc_val: The exception value.
            exc_tb: The exception traceback.
        """
        self.exit()

    def _pop(self) -> None:
        """Log and reset the counter."""
        log(self.logger, Point("counter", self.metric, self.value, self.tags))
        self.value = 0
        self.last_log_time = time()

    def increment(self, value: int = 1) -> None:
        """Increment the counter.

        Args:
            value: The value to increment by.
        """
        self.value += value
        if self._ready_to_log():
            self._pop()

    def _ready_to_log(self) -> bool:
        """Check if the counter is ready to log.

        Returns:
            True if the counter is ready to log.
        """
        return time() - self.last_log_time > self.log_interval


class Timer(Meter):
    """A meter for timing things."""

    def __init__(self, metric: Metric | str, tags: dict | None = None) -> None:
        """Initialize a timer.

        Args:
            metric: The metric type.
            tags: Tags to add to the measurement.
        """
        super().__init__(metric, tags)
        self.start_time = time()

    @override
    def __enter__(self) -> Self:
        """Enter the timer context.

        Returns:
            The timer instance.
        """
        self.start_time = time()
        return self

    @override
    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Exit the timer context.

        Args:
            exc_type: The exception type.
            exc_val: The exception value.
            exc_tb: The exception traceback.
        """
        if Tag.STATUS not in self.tags:
            self.tags[Tag.STATUS] = (
                Status.SUCCEEDED if exc_type is None else Status.FAILED
            )
        log(self.logger, Point("timer", self.metric, self.elapsed(), self.tags))

    def elapsed(self) -> float:
        """Get the elapsed time.

        Returns:
            The elapsed time.
        """
        return time() - self.start_time


def get_metrics_logger() -> logging.Logger:
    """Get a logger for emitting metrics.

    Returns:
        A logger that can be used to emit metrics.
    """
    return logging.getLogger(METRICS_LOGGER_NAME)


def record_counter(
    endpoint: str | None = None,
    log_interval: float = DEFAULT_LOG_INTERVAL,
    **tags: t.Any,
) -> Counter:
    """Use for counting records retrieved from a source.

    This is the legacy ``singer-python`` factory. If you are building on the
    full Meltano Singer SDK, use :func:`singer_sdk.metrics.record_counter`
    instead, which requires a stream name.

    with record_counter(endpoint="/users") as counter:
         for record in my_records:
             # Do something with the record
             counter.increment()

    Args:
        endpoint: The endpoint name.
        log_interval: The interval at which to log the count.
        tags: Tags to add to the measurement.

    Returns:
        A counter for counting records.
    """
    if endpoint:
        tags[Tag.ENDPOINT] = endpoint
    return Counter(Metric.RECORD_COUNT, tags, log_interval=log_interval)


def http_request_timer(endpoint: str | None) -> Timer:
    """Use for timing HTTP requests to an endpoint.

    with http_request_timer("/users") as timer:
        # Make a request

    Args:
        endpoint: The endpoint name.

    Returns:
        A timer for timing HTTP requests.
    """
    tags: dict[str, t.Any] = {}
    if endpoint:
        tags[Tag.ENDPOINT] = endpoint
    return Timer(Metric.HTTP_REQUEST_DURATION, tags)


def job_timer(job_type: str | None = None) -> Timer:
    """Use for timing asynchronous jobs.

    with job_timer("export") as timer:
        # Do the job

    Args:
        job_type: The job type.

    Returns:
        A timer for timing jobs.
    """
    tags: dict[str, t.Any] = {}
    if job_type:
        tags[Tag.JOB_TYPE] = job_type
    return Timer(Metric.JOB_DURATION, tags)
