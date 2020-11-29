"""Sample tap test for tap-snowflake."""

from pathlib import Path

from tap_base.tap_base import TapBase
from tap_base.tests.sample_tap_snowflake.connection import SampleTapSnowflakeConnection
from tap_base.tests.sample_tap_snowflake.stream import SampleTapSnowflakeStream


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
            connection_class=SampleTapSnowflakeConnection,
            state=state,
        )

    # Core plugin metadata:

    def create_stream(self, stream_id: str) -> SampleTapSnowflakeStream:
        return SampleTapSnowflakeStream(
            stream_id=stream_id, schema=None, properties=None
        )

    def initialize_stream_from_catalog(
        self,
        stream_id: str,
        friendly_name: str,
        schema: dict,
        metadata: dict,
        upstream_table_name: str,
    ) -> SampleTapSnowflakeStream:
        """Return a tap stream object."""
        return SampleTapSnowflakeStream(
            stream_id, friendly_name, schema, metadata, upstream_table_name
        )
