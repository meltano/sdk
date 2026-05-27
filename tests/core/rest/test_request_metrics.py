from __future__ import annotations

import logging
import sys
import typing as t

from singer_sdk import metrics
from singer_sdk.streams.rest import RESTStream

if sys.version_info >= (3, 12):
    from typing import override  # noqa: ICN003
else:
    from typing_extensions import override

if t.TYPE_CHECKING:
    import pytest
    import requests_mock

    from singer_sdk.streams.rest import HTTPRequest, PageContext
    from singer_sdk.tap_base import Tap


SCHEMA = {"type": "object", "properties": {"id": {"type": "integer"}}}


class _BaseTestStream(RESTStream):
    name = "test_stream"
    path = "/test"
    url_base = "https://example.com"
    schema = SCHEMA

    @override
    def get_http_request(
        self,
        *,
        page: PageContext[None],
    ) -> HTTPRequest:
        request = super().get_http_request(page=page)
        request.params["user_id"] = 1
        return request


def test_metrics_logging(
    requests_mock: requests_mock.Mocker,
    rest_tap: Tap,
    caplog: pytest.LogCaptureFixture,
):
    class TestStream(_BaseTestStream):
        _LOG_REQUEST_METRICS = True

    stream = TestStream(rest_tap)
    requests_mock.get("https://example.com/test?user_id=1", json=[{"id": 1}])

    with caplog.at_level(logging.INFO, logger="singer_sdk.metrics"):
        records = stream.get_records(None)

    assert list(records) == [{"id": 1}]
    assert len(caplog.records) == 2
    assert all(record.msg.startswith("METRIC") for record in caplog.records)

    assert caplog.records[0].args
    assert isinstance(caplog.records[0].args, tuple)
    point_1 = caplog.records[0].args[0]
    assert isinstance(point_1, metrics.Point)
    assert point_1.metric == "http_request_duration"
    assert point_1.tags["endpoint"] == "/test"
    assert "context" not in point_1.tags
    assert "url" not in point_1.tags

    assert caplog.records[1].args
    assert isinstance(caplog.records[1].args, tuple)
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
    assert isinstance(caplog.records[0].args, tuple)
    point = caplog.records[0].args[0]
    assert isinstance(point, metrics.Point)
    assert point.metric == "http_request_count"
    assert point.value == 1
