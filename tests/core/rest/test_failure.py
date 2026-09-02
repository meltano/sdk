from __future__ import annotations

import json
import typing as t
from contextlib import nullcontext
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime

import pytest
import requests

from singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from singer_sdk.streams.rest import RESTStream

if t.TYPE_CHECKING:
    from contextlib import AbstractContextManager


class CustomResponseValidationStream(RESTStream):
    """Stream with non-conventional error response."""

    url_base = "https://badapi.test"
    name = "imabadapi"
    schema: t.ClassVar[dict] = {"type": "object", "properties": {}}
    path = "/dummy"

    class StatusMessage:
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
    "status_code,reason,content,headers,expectation",
    [
        (
            400,
            "Bad request",
            None,
            {},
            pytest.raises(
                FatalAPIError,
                match=r"400 Client Error: Bad request for path: /dummy",
            ),
        ),
        (
            503,
            "Service Unavailable",
            None,
            {},
            pytest.raises(
                RetriableAPIError,
                match=r"503 Server Error: Service Unavailable for path: /dummy",
            ),
        ),
        (
            521,  # Cloudflare custom status code higher than max(HTTPStatus)
            "Web Server Is Down",
            None,
            {},
            pytest.raises(
                RetriableAPIError,
                match=r"521 Server Error: Web Server Is Down for path: /dummy",
            ),
        ),
        (
            429,
            "Too Many Requests",
            None,
            {},
            pytest.raises(
                RetriableAPIError,
                match=r"429 Client Error: Too Many Requests for path: /dummy",
            ),
        ),
        (
            403,
            "Forbidden",
            b'{"error": "Your token does not have the required scopes"}',
            {},
            pytest.raises(
                FatalAPIError,
                match=(
                    r"403 Client Error: Forbidden for path: /dummy, "
                    r'content is \{\"error": \"Your token does not have the required scopes\"\}'  # noqa: E501
                ),
            ),
        ),
        (
            403,
            "Forbidden",
            b"",
            {},
            pytest.raises(
                FatalAPIError,
                match=r"403 Client Error: Forbidden for path: /dummy",
            ),
        ),
        (
            429,
            "Too Many Requests",
            b"",
            {"Retry-After": "10"},
            pytest.raises(
                RetriableAPIError,
                match=r"429 Client Error: Too Many Requests for path: /dummy, headers: Retry-After",  # noqa: E501
            ),
        ),
        (200, "OK", b"OK", {}, nullcontext()),
    ],
    ids=[
        "client-error",
        "server-error-503",
        "server-error-521",
        "rate-limited",
        "forbidden-with-content",
        "forbidden-empty-content",
        "rate-limited-with-headers",
        "ok",
    ],
)
def test_status_code_validation(
    basic_rest_stream: RESTStream,
    status_code: int,
    reason: str,
    content: bytes | None,
    headers: dict[str, str],
    expectation,
):
    fake_response = requests.Response()
    fake_response.status_code = status_code
    fake_response.reason = reason
    fake_response._content = content
    fake_response.headers.update(headers)

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
def test_status_message_api(
    custom_validation_stream: CustomResponseValidationStream,
    message: str,
    expectation: AbstractContextManager,
):
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
    ],
    ids=[
        "default",
        "changed",
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


@pytest.mark.parametrize(
    "headers,expected",
    [
        ({"Retry-After": "30"}, 30.0),
        ({"X-RateLimit-Reset": "45"}, 45.0),
        # Retry-After takes precedence over X-RateLimit-Reset.
        ({"Retry-After": "30", "X-RateLimit-Reset": "999"}, 30.0),
        # Zero and negative waits are clamped to 0.0.
        ({"Retry-After": "0"}, 0.0),
        ({"Retry-After": "-5"}, 0.0),
        ({"X-RateLimit-Reset": "0"}, 0.0),
        ({"X-RateLimit-Reset": "-10"}, 0.0),
        # Unparsable / missing headers fall back to exponential backoff (None).
        ({"Retry-After": "not-a-number"}, None),
        ({"X-RateLimit-Reset": "not-a-number"}, None),
        ({}, None),
    ],
    ids=[
        "retry-after-seconds",
        "ratelimit-reset-seconds",
        "retry-after-precedence",
        "retry-after-zero",
        "retry-after-negative-clamped",
        "ratelimit-reset-zero",
        "ratelimit-reset-negative-clamped",
        "retry-after-unparsable",
        "ratelimit-reset-unparsable",
        "no-headers",
    ],
)
def test_get_wait_time_from_response(
    basic_rest_stream: RESTStream,
    headers: dict[str, str],
    expected: float | None,
):
    response = requests.Response()
    response.status_code = 429
    response.headers.update(headers)

    assert basic_rest_stream.get_wait_time_from_response(response) == expected


def test_get_wait_time_from_response_http_date(basic_rest_stream: RESTStream):
    """A ``Retry-After`` HTTP date is converted to seconds from now."""
    reset_at = datetime.now(tz=timezone.utc) + timedelta(seconds=120)
    response = requests.Response()
    response.status_code = 429
    response.headers.update({"Retry-After": format_datetime(reset_at, usegmt=True)})

    wait = basic_rest_stream.get_wait_time_from_response(response)

    assert wait is not None
    # Allow a small window for clock drift / test execution time.
    assert 110 <= wait <= 120


def test_get_wait_time_from_response_naive_http_date(basic_rest_stream: RESTStream):
    """A ``Retry-After`` date without timezone info is interpreted as UTC."""
    reset_at = datetime.now(tz=timezone.utc) + timedelta(seconds=120)
    # ``format_datetime`` on a naive datetime emits a ``-0000`` designator, which
    # ``parsedate_to_datetime`` parses back to a naive datetime.
    naive_http_date = format_datetime(reset_at.replace(tzinfo=None))
    response = requests.Response()
    response.status_code = 429
    response.headers.update({"Retry-After": naive_http_date})

    wait = basic_rest_stream.get_wait_time_from_response(response)

    assert wait is not None
    # Allow a small window for clock drift / test execution time.
    assert 110 <= wait <= 120


def _retriable(headers: dict[str, str] | None) -> RetriableAPIError:
    response = requests.Response()
    response.status_code = 429
    if headers:
        response.headers.update(headers)
    return RetriableAPIError("rate limited", response)


def test_backoff_wait_generator_respects_headers_and_falls_back(
    basic_rest_stream: RESTStream,
):
    """Header waits are respected; other errors fall back to exponential backoff."""
    gen = basic_rest_stream.backoff_wait_generator()
    gen.send(None)  # Prime the generator (matches backoff's initialization).

    # 429 with a rate-limit header -> respect the header's wait time.
    assert gen.send(_retriable({"Retry-After": "30"})) == 30

    # 429 with an unparsable header -> exponential fallback.
    assert gen.send(_retriable({"Retry-After": "not-a-number"})) == 2

    # Connection-level error has no response -> exponential fallback, no crash.
    assert gen.send(requests.exceptions.ConnectionError()) == 4

    # 429 without rate-limit headers -> exponential backoff continues.
    assert gen.send(_retriable(None)) == 8
