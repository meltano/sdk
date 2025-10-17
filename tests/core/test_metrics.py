from __future__ import annotations

import logging
import os
import typing as t

import pytest
import time_machine

from singer_sdk import metrics

if t.TYPE_CHECKING:
    from collections.abc import Callable, Iterable


class CustomObject:
    def __init__(self, name: str, value: int):
        self.name = name
        self.value = value

    def __str__(self) -> str:
        return f"{self.name}={self.value}"


@pytest.fixture
def metric_log_records() -> list[logging.LogRecord]:
    normal_log = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname="test.py",
        lineno=1,
        msg="Hey there",
        args=(),
        exc_info=None,
    )
    counter = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname="test.py",
        lineno=1,
        msg="METRIC: %s",
        args=(
            metrics.Point(
                metric_type="counter",
                metric=metrics.Metric.RECORD_COUNT,
                value=1,
                tags={
                    metrics.Tag.STREAM: "users",
                    metrics.Tag.CONTEXT: {
                        "account_id": 1,
                        "parent_id": 1,
                    },
                    "test_tag": "test_value",
                },
            ),
        ),
        exc_info=None,
    )

    timer = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname="test.py",
        lineno=1,
        msg="METRIC: %s",
        args=(
            metrics.Point(
                metric_type="timer",
                metric=metrics.Metric.SYNC_DURATION,
                value=0.314,
                tags={
                    metrics.Tag.STREAM: "teams",
                    metrics.Tag.CONTEXT: {
                        "account_id": 1,
                    },
                    "test_tag": "test_value",
                },
            ),
        ),
        exc_info=None,
    )

    return [normal_log, counter, timer]


def test_meter():
    pid = os.getpid()

    class _MyMeter(metrics.Meter):
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            pass

    meter = _MyMeter(metrics.Metric.RECORD_COUNT)

    assert meter.tags == {metrics.Tag.PID: pid}

    stream_context = {"parent_id": 1}
    meter.context = stream_context
    assert meter.tags == {
        metrics.Tag.CONTEXT: stream_context,
        metrics.Tag.PID: pid,
    }

    meter.context = None
    assert metrics.Tag.CONTEXT not in meter.tags


def test_record_counter(caplog: pytest.LogCaptureFixture):
    metrics_logger = logging.getLogger(metrics.METRICS_LOGGER_NAME)
    metrics_logger.propagate = True

    caplog.set_level(logging.INFO, logger=metrics.METRICS_LOGGER_NAME)
    pid = os.getpid()
    custom_object = CustomObject("test", 1)

    with metrics.record_counter(
        "test_stream",
        endpoint="test_endpoint",
        custom_tag="pytest",
        custom_obj=custom_object,
    ) as counter:
        for _ in range(100):
            counter.last_log_time = 0
            assert counter._ready_to_log()

            counter.increment()

    total = 0

    assert len(caplog.records) == 100 + 1
    # raise AssertionError

    for record in caplog.records:
        assert record.levelname == "INFO"
        assert record.msg.startswith("METRIC")

        assert record.args
        assert isinstance(record.args[0], metrics.Point)

        point = record.args[0].to_dict()
        assert point["type"] == "counter"
        assert point["metric"] == "record_count"
        assert point["tags"] == {
            metrics.Tag.STREAM: "test_stream",
            metrics.Tag.ENDPOINT: "test_endpoint",
            metrics.Tag.PID: pid,
            "custom_tag": "pytest",
            "custom_obj": custom_object,
        }

        total += point["value"]

    assert total == 100


def test_sync_timer(caplog: pytest.LogCaptureFixture):
    metrics_logger = logging.getLogger(metrics.METRICS_LOGGER_NAME)
    metrics_logger.propagate = True

    caplog.set_level(logging.INFO, logger=metrics.METRICS_LOGGER_NAME)

    pid = os.getpid()
    traveler = time_machine.travel(0, tick=False)
    traveler.start()

    with metrics.sync_timer("test_stream", custom_tag="pytest"):
        traveler.stop()

        traveler = time_machine.travel(10, tick=False)
        traveler.start()

    traveler.stop()

    record = caplog.records[0]
    assert record.levelname == "INFO"
    assert record.msg.startswith("METRIC")

    assert record.args
    assert isinstance(record.args[0], metrics.Point)

    point = record.args[0].to_dict()
    assert point["type"] == "timer"
    assert point["metric"] == "sync_duration"
    assert point["tags"] == {
        metrics.Tag.STREAM: "test_stream",
        metrics.Tag.STATUS: "succeeded",
        metrics.Tag.PID: pid,
        "custom_tag": "pytest",
    }

    assert pytest.approx(point["value"], rel=0.001) == 10.0


def _filter(
    records: Iterable[logging.LogRecord],
    filter_func: Callable[[logging.LogRecord], bool],
) -> list[logging.LogRecord]:
    return list(filter(filter_func, records))


def test_metric_filter_no_exclusions(
    metric_log_records: list[logging.LogRecord],
) -> None:
    metric_filter = metrics.MetricExclusionFilter()
    filtered = _filter(metric_log_records, metric_filter.filter)
    assert len(filtered) == 3


def test_metric_filter_malformed_point() -> None:
    metric_filter = metrics.MetricExclusionFilter()
    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname="test.py",
        lineno=1,
        msg="METRIC",
        args=(),
        exc_info=None,
    )
    record.__dict__["point"] = "Not a dict"

    assert metric_filter.filter(record)


def test_metric_filter_exclude_metrics(
    metric_log_records: list[logging.LogRecord],
) -> None:
    metric_filter = metrics.MetricExclusionFilter(metrics=["record_count"])
    filtered = _filter(metric_log_records, metric_filter.filter)
    assert len(filtered) == 2
    assert filtered[0].msg == "Hey there"
    assert filtered[1].msg == "METRIC: %s"
    assert filtered[1].args
    assert isinstance(filtered[1].args[0], metrics.Point)
    assert filtered[1].args[0].metric.value == "sync_duration"


def test_metric_filter_exclude_metric_types(
    metric_log_records: list[logging.LogRecord],
) -> None:
    metric_filter = metrics.MetricExclusionFilter(types=["timer"])
    filtered = _filter(metric_log_records, metric_filter.filter)
    assert len(filtered) == 2
    assert filtered[0].msg == "Hey there"
    assert filtered[1].msg == "METRIC: %s"
    assert filtered[1].args
    assert isinstance(filtered[1].args[0], metrics.Point)
    assert filtered[1].args[0].metric.value == "record_count"


def test_metric_filter_exclude_tags(
    metric_log_records: list[logging.LogRecord],
) -> None:
    metric_filter = metrics.MetricExclusionFilter(
        tags={
            metrics.Tag.STREAM: "users",
        },
    )
    filtered = _filter(metric_log_records, metric_filter.filter)
    assert len(filtered) == 2
    assert filtered[0].msg == "Hey there"
    assert filtered[1].args
    assert isinstance(filtered[1].args[0], metrics.Point)
    assert filtered[1].args[0].tags[metrics.Tag.STREAM] == "teams"

    metric_filter = metrics.MetricExclusionFilter(
        tags={
            "test_tag": "test_value",
        },
    )
    filtered = _filter(metric_log_records, metric_filter.filter)
    assert len(filtered) == 1
    assert filtered[0].msg == "Hey there"


@pytest.mark.xfail(reason="Nested tags are not supported yet")
def test_metric_filter_exclude_nested_tags(
    metric_log_records: list[logging.LogRecord],
) -> None:
    metric_filter = metrics.MetricExclusionFilter(
        tags={
            metrics.Tag.CONTEXT: {
                "account_id": 1,
            },
        },
    )
    filtered = _filter(metric_log_records, metric_filter.filter)
    assert len(filtered) == 1
    assert filtered[0].msg == "Hey there"


def test_metric_filter_exclude_name_and_type(
    metric_log_records: list[logging.LogRecord],
) -> None:
    metric_filter = metrics.MetricExclusionFilter(
        metrics=["record_count"],
        types=["counter"],
    )
    filtered = _filter(metric_log_records, metric_filter.filter)
    assert len(filtered) == 2
    assert filtered[0].msg == "Hey there"
    assert filtered[1].msg == "METRIC: %s"
    assert filtered[1].args
    assert isinstance(filtered[1].args[0], metrics.Point)
    assert filtered[1].args[0].metric.value == "sync_duration"


def test_metric_filter_exclude_name_and_tag(
    metric_log_records: list[logging.LogRecord],
) -> None:
    metric_filter = metrics.MetricExclusionFilter(
        metrics=["record_count"],
        tags={"stream": "users"},
    )
    filtered = _filter(metric_log_records, metric_filter.filter)
    assert len(filtered) == 2
