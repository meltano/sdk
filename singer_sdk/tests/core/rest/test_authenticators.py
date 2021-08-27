"""Tests for authentication helpers."""

import pytest

from singer_sdk.streams import RESTStream
from singer_sdk.tap_base import Tap


@pytest.mark.parametrize(
    "stream_name,other_stream_name,auth_reused",
    [
        (
            "some_stream",
            "some_stream",
            False,
        ),
        (
            "some_stream",
            "other_stream",
            False,
        ),
        (
            "single_auth_stream",
            "single_auth_stream",
            True,
        ),
        (
            "single_auth_stream",
            "reused_single_auth_stream",
            True,
        ),
        (
            "cached_auth_stream",
            "cached_auth_stream",
            True,
        ),
        (
            "cached_auth_stream",
            "other_cached_auth_stream",
            False,
        ),
    ],
    ids=[
        "naive-auth-not-reused-between-requests",
        "naive-auth-not-reused-between-streams",
        "singleton-auth-reused-between-requests",
        "singleton-auth-reused-between-streams",
        "cached-auth-reused-between-requests",
        "cached-auth-not-reused-between-streams",
    ],
)
def test_authenticator_is_reused(
    rest_tap: Tap, stream_name: str, other_stream_name: str, auth_reused: bool
):
    """Validate that the stream's authenticator is a singleton."""
    stream: RESTStream = rest_tap.streams[stream_name]
    other_stream: RESTStream = rest_tap.streams[other_stream_name]

    assert (stream.authenticator is other_stream.authenticator) is auth_reused
