"""REST fixtures."""

import pytest

from singer_sdk.streams import RESTStream
from singer_sdk.tap_base import Tap


class SimpleRESTStream(RESTStream):
    url_base = "https://example.com"
    schema = {
        "type": "object",
        "properties": {},
    }


class SimpleTap(Tap):
    name = "tappy"

    def discover_streams(self):
        """Get collection of streams."""
        return [
            SimpleRESTStream(self, name="some_stream"),
            SimpleRESTStream(self, name="other_stream"),
        ]


@pytest.fixture
def rest_tap():
    return SimpleTap()
