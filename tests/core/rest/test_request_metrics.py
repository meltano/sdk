from __future__ import annotations

import logging
import typing as t

import pytest

from singer_sdk import metrics
from singer_sdk.streams.rest import RESTStream

if t.TYPE_CHECKING:
    import requests_mock

    from singer_sdk.tap_base import Tap


SCHEMA = {"type": "object", "properties": {"id": {"type": "integer"}}}


class _BaseTestStream(RESTStream):
    name = "test_stream"
    path = "/test"
    url_base = "https://example.com"
    schema = SCHEMA

    def get_url_params(self, *args: t.Any, **kwargs: t.Any) -> dict[str, t.Any] | str:  # noqa: ARG002
        return {"user_id": 1}


@pytest.mark.parametrize("context", [None, {"partition": "p1"}])
@pytest.mark.parametrize("log_urls", [True, False])
def test_metrics_logging(
    requests_mock: requests_mock.Mocker,
    rest_tap: Tap,
    caplog: pytest.LogCaptureFixture,
    context: dict | None,
    log_urls: bool,
):
    class TestStream(_BaseTestStream):
        _LOG_REQUEST_METRICS = True
        _LOG_REQUEST_METRIC_URLS = log_urls

    stream = TestStream(rest_tap)
    requests_mock.get("https://example.com/test?user_id=1", json=[{"id": 1}])

    with caplog.at_level(logging.INFO, logger="singer_sdk.metrics"):
        records = stream.get_records(context)

    assert list(records) == [{"id": 1}]
    assert len(caplog.records) == 2
    assert all(record.msg.startswith("METRIC") for record in caplog.records)

    assert caplog.records[0].args
    point_1 = caplog.records[0].args[0]
    assert isinstance(point_1, metrics.Point)
    assert point_1.metric == "http_request_duration"
    assert point_1.tags["endpoint"] == "/test"
    assert point_1.tags.get("context") == context
    assert (
        (log_urls and point_1.tags.get("url") == "/test?user_id=1")  # Log URLs
        or (not log_urls and "url" not in point_1.tags)  # Don't log URLs
    )

    assert caplog.records[1].args
    point_2 = caplog.records[1].args[0]
    assert isinstance(point_2, metrics.Point)
    assert point_2.metric == "http_request_count"
    assert point_2.value == 1


def test_disable_request_metrics(
    requests_mock: requests_mock.Mocker,
    rest_tap: Tap,
    caplog: pytest.LogCaptureFixture,
):
    class TestStream(_BaseTestStream):
        _LOG_REQUEST_METRICS = False

    stream = TestStream(rest_tap)
    requests_mock.get("https://example.com/test?user_id=1", json=[{"id": 1}])

    with caplog.at_level(logging.INFO, logger="singer_sdk.metrics"):
        records = stream.get_records(None)

    assert list(records) == [{"id": 1}]
    assert len(caplog.records) == 1
    assert caplog.records[0].msg.startswith("METRIC")

    assert caplog.records[0].args
    point = caplog.records[0].args[0]
    assert isinstance(point, metrics.Point)
    assert point.metric == "http_request_count"
    assert point.value == 1
