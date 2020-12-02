"""Sample tap stream test for tap-pardot."""

from tap_base import TapStreamBase


class SampleTapPardotStream(TapStreamBase):
    """Sample tap test for pardot."""

    def __init__(self, stream_id: str, schema: dict, properties: dict = None):
        """Initialize stream class."""
        pass
