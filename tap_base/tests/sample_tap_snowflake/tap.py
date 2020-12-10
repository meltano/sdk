"""Sample tap test for tap-snowflake."""

from pathlib import Path
from typing import Type

from tap_base.tap_base import TapBase
from tap_base.tests.sample_tap_snowflake.tap_stream import SampleTapSnowflakeStream


PLUGIN_NAME = "sample-tap-snowflake"
PLUGIN_VERSION_FILE = "./VERSION"
PLUGIN_CAPABILITIES = [
    "sync",
    "catalog",
    "discover",
    "state",
]
ACCEPTED_CONFIG = [
    "account",
    "dbname",
    "user",
    "password",
    "warehouse",
    "tables",
]
REQUIRED_CONFIG_SETS = [
    ["account", "dbname", "user", "password", "warehouse", "tables"]
]


class SampleTapSnowflake(TapBase):
    """Sample tap for Snowflake."""

    @property
    @classmethod
    def stream_class(cls) -> Type[SampleTapSnowflakeStream]:
        return SampleTapSnowflakeStream
