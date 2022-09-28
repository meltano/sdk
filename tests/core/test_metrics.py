import logging
import time

import pytest

from singer_sdk import metrics


def test_meter():
    class _MyMeter(metrics.Meter):
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            pass

    meter = _MyMeter(metrics.Metric.RECORD_COUNT)

    assert meter.tags == {}

    stream_context = {"parent_id": 1}
    meter.context = stream_context
    assert meter.tags == {metrics.Tag.CONTEXT: stream_context}

    meter.context = None
    assert metrics.Tag.CONTEXT not in meter.tags


def test_record_counter(caplog: pytest.LogCaptureFixture):
    caplog.set_level(logging.INFO, logger=metrics.METRICS_LOGGER_NAME)
    with metrics.record_counter(
        "test_stream",
        endpoint="test_endpoint",
        custom_tag="pytest",
    ) as counter:
        for _ in range(100):
            counter.last_log_time = 0
            assert counter._ready_to_log()

            counter.increment()

    total = 0

    assert len(caplog.records) == 100 + 1

    for record in caplog.records:
        assert record.levelname == "INFO"
        assert record.msg == "INFO METRIC: %s"

        point: metrics.Point[int] = record.args[0]
        assert point.metric_type == "counter"
        assert point.metric == "record_count"
        assert point.tags == {
            metrics.Tag.STREAM: "test_stream",
            metrics.Tag.ENDPOINT: "test_endpoint",
            "custom_tag": "pytest",
        }

        total += point.value

    assert total == 100


def test_sync_timer(caplog: pytest.LogCaptureFixture):
    caplog.set_level(logging.INFO, logger=metrics.METRICS_LOGGER_NAME)
    with metrics.sync_timer("test_stream", custom_tag="pytest") as timer:
        start_time = timer.start_time
        for _ in range(1000):
            time.sleep(0.001)
        end_time = time.time()

    assert len(caplog.records) == 1

    record = caplog.records[0]
    assert record.levelname == "INFO"
    assert record.msg == "INFO METRIC: %s"

    point: metrics.Point[float] = record.args[0]
    assert point.metric_type == "timer"
    assert point.metric == "sync_duration"
    assert point.tags == {
        metrics.Tag.STREAM: "test_stream",
        metrics.Tag.STATUS: "succeeded",
        "custom_tag": "pytest",
    }

    assert pytest.approx(point.value, rel=0.001) == end_time - start_time
