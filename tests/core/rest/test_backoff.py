from __future__ import annotations

import json
import typing as t

try:
    from contextlib import nullcontext
except ImportError:
    from contextlib2 import nullcontext

from enum import Enum

import pytest
import requests

from singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from singer_sdk.streams.rest import RESTStream


class CustomResponseValidationStream(RESTStream):
    """Stream with non-conventional error response."""

    url_base = "https://badapi.test"
    name = "imabadapi"
    schema: t.ClassVar[dict] = {"type": "object", "properties": {}}
    path = "/dummy"

    class StatusMessage(str, Enum):
        """Possible status messages."""

        OK = "OK"
        ERROR = "ERROR"
        UNAVAILABLE = "UNAVAILABLE"

    def validate_response(self, response: requests.Response):
        super().validate_response(response)
        data = response.json()
        if data["status"] == self.StatusMessage.ERROR:
            msg = "Error message found :("
            raise FatalAPIError(msg)
        if data["status"] == self.StatusMessage.UNAVAILABLE:
            msg = "API is unavailable"
            raise RetriableAPIError(msg)


@pytest.fixture
def basic_rest_stream(rest_tap):
    """Get a RESTful tap stream."""
    return rest_tap.streams["some_stream"]


@pytest.fixture
def custom_validation_stream(rest_tap):
    """Get a RESTful tap stream with custom response validation."""
    return CustomResponseValidationStream(rest_tap)


@pytest.mark.parametrize(
    "status_code,reason,expectation",
    [
        (
            400,
            "Bad request",
            pytest.raises(
                FatalAPIError,
                match=r"400 Client Error: Bad request for path: /dummy",
            ),
        ),
        (
            503,
            "Service Unavailable",
            pytest.raises(
                RetriableAPIError,
                match=r"503 Server Error: Service Unavailable for path: /dummy",
            ),
        ),
        (
            521,  # Cloudflare custom status code higher than max(HTTPStatus)
            "Web Server Is Down",
            pytest.raises(
                RetriableAPIError,
                match=r"521 Server Error: Web Server Is Down for path: /dummy",
            ),
        ),
        (
            429,
            "Too Many Requests",
            pytest.raises(
                RetriableAPIError,
                match=r"429 Client Error: Too Many Requests for path: /dummy",
            ),
        ),
        (200, "OK", nullcontext()),
    ],
    ids=["client-error", "server-error", "server-error", "rate-limited", "ok"],
)
def test_status_code_api(basic_rest_stream, status_code, reason, expectation):
    fake_response = requests.Response()
    fake_response.status_code = status_code
    fake_response.reason = reason

    with expectation:
        basic_rest_stream.validate_response(fake_response)


@pytest.mark.parametrize(
    "message,expectation",
    [
        (
            CustomResponseValidationStream.StatusMessage.ERROR,
            pytest.raises(FatalAPIError),
        ),
        (
            CustomResponseValidationStream.StatusMessage.UNAVAILABLE,
            pytest.raises(RetriableAPIError),
        ),
        (
            CustomResponseValidationStream.StatusMessage.OK,
            nullcontext(),
        ),
    ],
    ids=["fatal", "retriable", "ok"],
)
def test_status_message_api(custom_validation_stream, message, expectation):
    fake_response = requests.Response()
    fake_response.status_code = 200
    fake_response._content = json.dumps({"status": message}).encode()
    fake_response.url = custom_validation_stream.url_base

    with expectation:
        custom_validation_stream.validate_response(fake_response)


@pytest.mark.parametrize(
    "rate_limit_codes,response_status,expectation",
    [
        (
            CustomResponseValidationStream.extra_retry_statuses,
            429,
            pytest.raises(RetriableAPIError),
        ),
        (
            [429, 403],
            403,
            pytest.raises(RetriableAPIError),
        ),
        (
            [],
            429,
            pytest.raises(FatalAPIError),
        ),
    ],
    ids=[
        "default",
        "changed",
        "missing",
    ],
)
def test_rate_limiting_status_override(
    basic_rest_stream,
    rate_limit_codes,
    response_status,
    expectation,
):
    fake_response = requests.Response()
    fake_response.status_code = response_status
    basic_rest_stream.extra_retry_statuses = rate_limit_codes

    with expectation:
        basic_rest_stream.validate_response(fake_response)
