"""Singer metrics logging."""

from __future__ import annotations

import abc
import enum
import json
import logging
import logging.config
import os
from dataclasses import asdict, dataclass, field
from pathlib import Path
from time import time
from types import TracebackType
from typing import Any, Generic, Mapping, TypeVar

import yaml

from singer_sdk.helpers._resources import Traversable, get_package_files

DEFAULT_LOG_INTERVAL = 60.0
METRICS_LOGGER_NAME = __name__
METRICS_LOG_LEVEL_SETTING = "metrics_log_level"

_TVal = TypeVar("_TVal")


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


class Metric(str, enum.Enum):
    """Common metric types."""

    RECORD_COUNT = "record_count"
    BATCH_COUNT = "batch_count"
    HTTP_REQUEST_DURATION = "http_request_duration"
    HTTP_REQUEST_COUNT = "http_request_count"
    JOB_DURATION = "job_duration"
    SYNC_DURATION = "sync_duration"


@dataclass
class Point(Generic[_TVal]):
    """An individual metric measurement."""

    metric_type: str
    metric: Metric
    value: _TVal
    tags: dict[str, Any] = field(default_factory=dict)

    def __str__(self) -> str:
        """Get string representation of this measurement.

        Returns:
            A string representation of this measurement.
        """
        return self.to_json()

    def to_json(self) -> str:
        """Convert this measure to a JSON object.

        Returns:
            A JSON object.
        """
        return json.dumps(asdict(self))


def log(logger: logging.Logger, point: Point) -> None:
    """Log a measurement.

    Args:
        logger: An logger instance.
        point: A measurement.
    """
    logger.info("INFO METRIC: %s", point)


class Meter(metaclass=abc.ABCMeta):
    """Base class for all meters."""

    def __init__(self, metric: Metric, tags: dict | None = None) -> None:
        """Initialize a meter.

        Args:
            metric: The metric type.
            tags: Tags to add to the measurement.
        """
        self.metric = metric
        self.tags = tags or {}
        self.logger = get_metrics_logger()

    @property
    def context(self) -> dict | None:
        """Get the context for this meter.

        Returns:
            A context dictionary.
        """
        return self.tags.get(Tag.CONTEXT)

    @context.setter
    def context(self, value: dict | None) -> None:
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
        metric: Metric,
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

    def __enter__(self) -> Counter:
        """Enter the counter context.

        Returns:
            The counter instance.
        """
        self.last_log_time = time()
        return self

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
        self._pop()

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

    def __init__(self, metric: Metric, tags: dict | None = None) -> None:
        """Initialize a timer.

        Args:
            metric: The metric type.
            tags: Tags to add to the measurement.
        """
        super().__init__(metric, tags)
        self.start_time = time()

    def __enter__(self) -> Timer:
        """Enter the timer context.

        Returns:
            The timer instance.
        """
        self.start_time = time()
        return self

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
            if exc_type is None:
                self.tags[Tag.STATUS] = Status.SUCCEEDED
            else:
                self.tags[Tag.STATUS] = Status.FAILED
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
    stream: str,
    endpoint: str | None = None,
    log_interval: float = DEFAULT_LOG_INTERVAL,
    **tags: Any,
) -> Counter:
    """Use for counting records retrieved from the source.

    with record_counter("my_stream", endpoint="/users") as counter:
         for record in my_records:
             # Do something with the record
             counter.increment()

    Args:
        stream: The stream name.
        endpoint: The endpoint name.
        log_interval: The interval at which to log the count.
        tags: Tags to add to the measurement.

    Returns:
        A counter for counting records.
    """
    tags[Tag.STREAM] = stream
    if endpoint:
        tags[Tag.ENDPOINT] = endpoint
    return Counter(Metric.RECORD_COUNT, tags, log_interval=log_interval)


def batch_counter(stream: str, **tags: Any) -> Counter:
    """Use for counting batches sent to the target.

    with batch_counter("my_stream") as counter:
         for batch in my_batches:
             # Do something with the batch
             counter.increment()

    Args:
        stream: The stream name.
        tags: Tags to add to the measurement.

    Returns:
        A counter for counting batches.
    """
    tags[Tag.STREAM] = stream
    return Counter(Metric.BATCH_COUNT, tags)


def http_request_counter(
    stream: str,
    endpoint: str,
    log_interval: float = DEFAULT_LOG_INTERVAL,
    **tags: Any,
) -> Counter:
    """Use for counting HTTP requests.

    with http_request_counter() as counter:
         for record in my_records:
             # Do something with the record
             counter.increment()

    Args:
        stream: The stream name.
        endpoint: The endpoint name.
        log_interval: The interval at which to log the count.
        tags: Tags to add to the measurement.

    Returns:
        A counter for counting HTTP requests.
    """
    tags.update({Tag.STREAM: stream, Tag.ENDPOINT: endpoint})
    return Counter(Metric.HTTP_REQUEST_COUNT, tags, log_interval=log_interval)


def sync_timer(stream: str, **tags: Any) -> Timer:
    """Use for timing the sync of a stream.

    with singer.metrics.sync_timer() as timer:
         # Do something
         print(f"Sync took {timer.elapsed()} seconds")

    Args:
        stream: The stream name.
        tags: Tags to add to the measurement.

    Returns:
        A timer for timing the sync of a stream.
    """
    tags[Tag.STREAM] = stream
    return Timer(Metric.SYNC_DURATION, tags)


def _load_yaml_logging_config(path: Traversable | Path) -> Any:  # noqa: ANN401
    """Load the logging config from the YAML file.

    Args:
        path: A path to the YAML file.

    Returns:
        The logging config.
    """
    with path.open() as f:
        return yaml.safe_load(f)


def _get_default_config() -> Any:  # noqa: ANN401
    """Get a logging configuration.

    Returns:
        A logging configuration.
    """
    log_config_path = get_package_files("singer_sdk").joinpath("default_logging.yml")
    return _load_yaml_logging_config(log_config_path)


def _setup_logging(config: Mapping[str, Any]) -> None:
    """Setup logging.

    Args:
        config: A plugin configuration dictionary.
    """
    logging.config.dictConfig(_get_default_config())

    config = config or {}
    metrics_log_level = config.get(METRICS_LOG_LEVEL_SETTING, "INFO").upper()
    logging.getLogger(METRICS_LOGGER_NAME).setLevel(metrics_log_level)

    if "SINGER_SDK_LOG_CONFIG" in os.environ:
        log_config_path = Path(os.environ["SINGER_SDK_LOG_CONFIG"])
        logging.config.dictConfig(_load_yaml_logging_config(log_config_path))
