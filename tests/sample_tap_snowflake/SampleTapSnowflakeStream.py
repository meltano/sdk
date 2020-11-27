"""Sample tap stream test for tap-snowflake."""

from singer import Schema
from tap_base import TapStreamBase


class SampleTapSnowflakeStream(TapStreamBase):
    """Sample tap test for snowflake."""

    def __init__(self, stream_id: str, schema: Schema):
        """Initialize stream class."""
        pass
