"""Test the legacy singer-python metrics factories."""

from __future__ import annotations

import json
import logging

import pytest

import singer.metrics
import singer_sdk.metrics


@pytest.fixture
def caplog_metrics(caplog: pytest.LogCaptureFixture):
    caplog.set_level(logging.INFO, logger=singer.metrics.METRICS_LOGGER_NAME)
    return caplog


def _points(caplog: pytest.LogCaptureFixture) -> list[dict]:
    return [
        json.loads(record.message.removeprefix("METRIC: "))
        for record in caplog.records
        if record.message.startswith("METRIC: ")
    ]


def test_metrics_logger_name():
    assert singer.metrics.METRICS_LOGGER_NAME == "singer_sdk.metrics"


def test_record_counter(caplog_metrics: pytest.LogCaptureFixture):
    with singer.metrics.record_counter(endpoint="/users") as counter:
        for _ in range(5):
            counter.increment()

    point = _points(caplog_metrics)[-1]
    assert point["type"] == "counter"
    assert point["metric"] == "record_count"
    assert point["value"] == 5
    assert point["tags"]["endpoint"] == "/users"
    assert "pid" in point["tags"]


def test_http_request_timer(caplog_metrics: pytest.LogCaptureFixture):
    with singer.metrics.http_request_timer("/users"):
        pass

    point = _points(caplog_metrics)[-1]
    assert point["type"] == "timer"
    assert point["metric"] == "http_request_duration"
    assert point["tags"]["endpoint"] == "/users"
    assert point["tags"]["status"] == "succeeded"


def test_job_timer_failed(caplog_metrics: pytest.LogCaptureFixture):
    msg = "boom"
    with pytest.raises(RuntimeError), singer.metrics.job_timer("export"):
        raise RuntimeError(msg)

    point = _points(caplog_metrics)[-1]
    assert point["type"] == "timer"
    assert point["metric"] == "job_duration"
    assert point["tags"]["job_type"] == "export"
    assert point["tags"]["status"] == "failed"


def test_counter_accepts_plain_string_metric(
    caplog_metrics: pytest.LogCaptureFixture,
):
    """Legacy code constructs Counter with a plain string metric name."""
    with singer.metrics.Counter("my_custom_count") as counter:
        counter.increment(3)

    point = _points(caplog_metrics)[-1]
    assert point["metric"] == "my_custom_count"
    assert point["value"] == 3


def test_shim_identity():
    assert singer_sdk.metrics.Counter is singer.metrics.Counter
    assert singer_sdk.metrics.Timer is singer.metrics.Timer
    assert singer_sdk.metrics.Point is singer.metrics.Point
    assert (
        singer_sdk.metrics.MetricExclusionFilter is singer.metrics.MetricExclusionFilter
    )
