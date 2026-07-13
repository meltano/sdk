"""Tests for GraphQL response validation."""

from __future__ import annotations

import json
import typing as t
from contextlib import nullcontext

import pytest
import requests

from singer_sdk.exceptions import FatalAPIError
from singer_sdk.streams.graphql import GraphQLStream

if t.TYPE_CHECKING:
    from contextlib import AbstractContextManager


class SimpleGraphQLStream(GraphQLStream):
    """Minimal GraphQL stream for testing."""

    name = "thestream"
    url_base = "https://example.com"
    schema: t.ClassVar[dict] = {"type": "object", "properties": {}}
    query = "{ thestream { id } }"


@pytest.fixture
def graphql_stream(rest_tap):
    """Create a GraphQL stream instance."""
    return SimpleGraphQLStream(rest_tap)


def _make_response(body: object) -> requests.Response:
    """Build a fake HTTP 200 response with a JSON body."""
    response = requests.Response()
    response.status_code = 200
    response._content = json.dumps(body).encode()
    return response


def _make_raw_response(content: bytes) -> requests.Response:
    """Build a fake HTTP 200 response with a raw, non-JSON body."""
    response = requests.Response()
    response.status_code = 200
    response._content = content
    return response


@pytest.mark.parametrize(
    "body,expectation",
    [
        (
            # errors present, no data -- should raise FatalAPIError
            {
                "errors": [{"message": "Your token has insufficient scope"}],
                "data": None,
            },
            pytest.raises(
                FatalAPIError,
                match="GraphQL errors in response: Your token has insufficient scope",
            ),
        ),
        (
            # multiple errors -- all messages aggregated, in order
            {
                "errors": [
                    {"message": "First error"},
                    {"message": "Second error"},
                ],
                "data": None,
            },
            pytest.raises(
                FatalAPIError,
                match="GraphQL errors in response: First error, Second error",
            ),
        ),
        (
            # errors present, data key absent -- should raise FatalAPIError
            {
                "errors": [{"message": "Bad Request"}],
            },
            pytest.raises(
                FatalAPIError,
                match="GraphQL errors in response: Bad Request",
            ),
        ),
        (
            # errors present alongside data -- partial success, should NOT raise
            {
                "errors": [{"message": "deprecated field used"}],
                "data": {"thestream": [{"id": 1}]},
            },
            nullcontext(),
        ),
        (
            # errors present, data is an empty object -- execution completed,
            # so this is partial success and should NOT raise
            {
                "errors": [{"message": "field error"}],
                "data": {},
            },
            nullcontext(),
        ),
        (
            # no errors -- normal response, should NOT raise
            {
                "data": {"thestream": [{"id": 1}]},
            },
            nullcontext(),
        ),
        (
            # empty errors list -- treated as no errors, should NOT raise
            {
                "errors": [],
                "data": {"thestream": [{"id": 1}]},
            },
            nullcontext(),
        ),
    ],
)
def test_graphql_validate_response(
    graphql_stream: SimpleGraphQLStream,
    body: dict,
    expectation: AbstractContextManager,
):
    """validate_response raises FatalAPIError on GraphQL errors with no data."""
    response = _make_response(body)
    with expectation:
        graphql_stream.validate_response(response)


@pytest.mark.parametrize(
    "errors,expected_message",
    [
        pytest.param(
            ["plain string error"],
            "plain string error",
            id="non-dict-entry",
        ),
        pytest.param(
            [{"code": "FORBIDDEN"}],
            "{'code': 'FORBIDDEN'}",
            id="dict-entry-without-message",
        ),
        pytest.param(
            "top-level error string",
            "top-level error string",
            id="non-list-errors",
        ),
    ],
)
def test_graphql_validate_response_nonstandard_errors(
    graphql_stream: SimpleGraphQLStream,
    errors: object,
    expected_message: str,
):
    """Nonstandard ``errors`` shapes are stringified rather than dropped."""
    response = _make_response({"errors": errors, "data": None})
    with pytest.raises(FatalAPIError) as excinfo:
        graphql_stream.validate_response(response)
    assert expected_message in str(excinfo.value)


def test_graphql_validate_response_invalid_json(
    graphql_stream: SimpleGraphQLStream,
):
    """A non-JSON body raises FatalAPIError instead of an unhandled error."""
    response = _make_raw_response(b"<html>Bad Gateway</html>")
    with pytest.raises(
        FatalAPIError,
        match="GraphQL response body is not valid JSON",
    ) as excinfo:
        graphql_stream.validate_response(response)
    assert isinstance(excinfo.value.__cause__, ValueError)


def test_graphql_validate_response_non_object_body(
    graphql_stream: SimpleGraphQLStream,
):
    """A JSON body that is not an object raises FatalAPIError."""
    response = _make_response([{"data": None}])
    with pytest.raises(
        FatalAPIError,
        match="GraphQL response body must be a JSON object",
    ):
        graphql_stream.validate_response(response)


def test_graphql_validate_response_warns_on_partial_success(
    graphql_stream: SimpleGraphQLStream,
    caplog: pytest.LogCaptureFixture,
):
    """validate_response logs a warning when errors and data are both present."""
    body = {
        "errors": [{"message": "deprecated field used"}],
        "data": {"thestream": [{"id": 1}]},
    }
    response = _make_response(body)
    graphql_stream.validate_response(response)
    assert any("deprecated field used" in record.message for record in caplog.records)
