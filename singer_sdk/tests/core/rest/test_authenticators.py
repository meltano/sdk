"""Tests for authentication helpers."""

from singer_sdk.streams import RESTStream
from singer_sdk.tap_base import Tap


def test_authenticator_is_reused(rest_tap: Tap):
    """Validate that the stream's authenticator is a singleton."""
    stream: RESTStream = rest_tap.streams["some_stream"]
    other_stream: RESTStream = rest_tap.streams["other_stream"]

    assert stream.authenticator is stream.authenticator
    assert other_stream.authenticator is other_stream.authenticator
    assert stream.authenticator is not other_stream.authenticator
