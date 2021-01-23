"""Module test for tap-snowflake functionality."""

from singer_sdk.samples.sample_tap_snowflake.snowflake_tap import SampleTapSnowflake
from singer_sdk.samples.sample_tap_snowflake.snowflake_tap_stream import (
    SampleTapSnowflakeStream,
)

__all__ = [
    "SampleTapSnowflake",
    "SampleTapSnowflakeStream",
    "SampleSnowflakeConnection",
]
