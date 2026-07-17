"""Singer metrics logging.

The metrics implementation lives in :mod:`singer.metrics` from
``meltano-singer-python``. This module re-exports it and adds the SDK-flavored
meter factories, which require a stream name.
"""

from __future__ import annotations

import typing as t

from singer.metrics import (
    DEFAULT_LOG_INTERVAL,
    METRICS_LOGGER_NAME,
    Counter,
    Meter,
    Metric,
    MetricExclusionFilter,
    Point,
    Status,
    Tag,
    Timer,
    get_metrics_logger,
    log,
    record_counter,
)

__all__ = [
    "DEFAULT_LOG_INTERVAL",
    "METRICS_LOGGER_NAME",
    "Counter",
    "Meter",
    "Metric",
    "MetricExclusionFilter",
    "Point",
    "Status",
    "Tag",
    "Timer",
    "batch_counter",
    "get_metrics_logger",
    "http_request_counter",
    "log",
    "record_counter",
    "sync_timer",
]


def batch_counter(stream: str, **tags: t.Any) -> Counter:
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
    **tags: t.Any,
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


def sync_timer(stream: str, **tags: t.Any) -> Timer:
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
