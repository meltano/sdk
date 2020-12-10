"""Sample tap test for tap-snowflake."""

from pathlib import Path
from typing import List, Type

from tap_base.tap_base import TapBase
from tap_base.tests.sample_tap_snowflake.tap_stream import SampleTapSnowflakeStream

from tap_base.helpers import classproperty


PLUGIN_NAME = "sample-tap-snowflake"
ACCEPTED_CONFIG_OPTIONS = [
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

    @classproperty
    def plugin_name(cls) -> str:
        """Return the plugin name."""
        return PLUGIN_NAME

    @classproperty
    def accepted_config_options(cls) -> List[str]:
        return ACCEPTED_CONFIG_OPTIONS

    @classproperty
    def required_config_sets(cls) -> List[List[str]]:
        return REQUIRED_CONFIG_SETS

    @classproperty
    def stream_class(cls) -> Type[SampleTapSnowflakeStream]:
        return SampleTapSnowflakeStream
