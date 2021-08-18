"""Tests for authentication helpers."""

from singer_sdk.streams import RESTStream
from singer_sdk.tap_base import Tap


def test_authenticator_is_reused(rest_tap: Tap):
    """Validate that the stream's authenticator is a singleton."""
    stream: RESTStream = rest_tap.streams["example"]

    assert stream.authenticator is stream.authenticator
