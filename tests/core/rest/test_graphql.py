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


def _make_response(body: dict) -> requests.Response:
    """Build a fake HTTP 200 response with a JSON body."""
    response = requests.Response()
    response.status_code = 200
    response._content = json.dumps(body).encode()
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
            # errors present alongside data -- partial success, should NOT raise
            {
                "errors": [{"message": "deprecated field used"}],
                "data": {"thestream": [{"id": 1}]},
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
