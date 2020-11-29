"""Sample tap test for tap-snowflake."""

from pathlib import Path
from tests.sample_tap_snowflake.SampleTapSnowfakeConnection import (
    SampleTapSnowflakeConnection,
)
from typing import Any, Dict, List, Tuple

from tap_base import TapBase
from sample_tap_snowflake import (
    SampleTapSnowflakeStream,
    SampleTapSnowflakeConnection,
    utils,
)
import snowflake.connector


PLUGIN_NAME = "sample-tap-snowflake"
PLUGIN_VERSION_FILE = "resources/VERSION"
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


class TooManyRecordsException(Exception):
    """Exception to raise when query returns more records than max_records."""


class TapSnowflake(TapBase):
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

    def get_available_stream_ids(self) -> List[str]:
        """Return a list of all streams (tables)."""
        conn = self.get_connection()
        conn.ensure_connected()
        results = conn.query(
            """SELECT catalog, schema_name, table_name from information_schema.tables"""
        )
        return [
            utils.concatenate_tap_stream_id(table, catalog, schema)
            for catalog, schema, table in results
        ]

    def initialize_stream_from_catalog(
        self,
        stream_id: str,
        friendly_name: str,
        schema: dict,
        metadata: dict,
        upstream_table_name: str,
    ) -> SampleTapSnowflakeStream:
        """Return a tap stream object."""
        conn = self.get_connection()
        return SampleTapSnowflakeStream(
            stream_id, friendly_name, schema, metadata, upstream_table_name
        )

    # Snowflake-specific functions:

    def open_connection(self):
        """Connect to snowflake database."""
        self._conn = snowflake.connector.connect(
            user=self.get_config("user"),
            password=self.get_config("password"),
            account=self.get_config("account"),
            database=self.get_config("dbname"),
            warehouse=self.get_config("warehouse"),
            insecure_mode=self.get_config("insecure_mode", False)
            # Use insecure mode to avoid "Failed to get OCSP response" warnings
            # insecure_mode=True
        )
        return self._conn
