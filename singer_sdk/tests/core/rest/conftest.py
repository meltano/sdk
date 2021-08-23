"""REST fixtures."""

import pytest

from singer_sdk.streams import RESTStream
from singer_sdk.tap_base import Tap


class SimpleRESTStream(RESTStream):
    """A REST stream for testing."""

    url_base = "https://example.com"
    schema = {
        "type": "object",
        "properties": {},
    }


class SimpleTap(Tap):
    """A REST tap for testing."""

    name = "tappy"

    def discover_streams(self):
        """Get collection of streams."""
        return [
            SimpleRESTStream(self, name="some_stream"),
            SimpleRESTStream(self, name="other_stream"),
        ]


@pytest.fixture
def rest_tap():
    """A RESTful tap instance fixture."""
    return SimpleTap()
