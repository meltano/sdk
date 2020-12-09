"""Sample tap test for tap-snowflake."""

from pathlib import Path

from tap_base.tap_base import TapBase
from tap_base.tests.sample_tap_snowflake.connection import SampleSnowflakeConnection
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

    def __init__(self, config: dict, state: dict = None) -> None:
        """Initialize the tap."""
        vers = Path(PLUGIN_VERSION_FILE).read_text()
        super().__init__(
            plugin_name=PLUGIN_NAME,
            version=vers,
            capabilities=PLUGIN_CAPABILITIES,
            accepted_options=ACCEPTED_CONFIG,
            option_set_requirements=REQUIRED_CONFIG_SETS,
            config=config,
            connection_class=SampleSnowflakeConnection,
            stream_class=SampleTapSnowflakeStream,
            state=state,
        )
