"""Sample tap test for tap-snowflake."""

from pathlib import Path
from typing import Any, Dict, List, Tuple

from tap_base import TapBase
from sample_tap_snowflake.SampleTapSnowflakeStream import SampleTapSnowflakeStream
import snowflake.connector


PLUGIN_NAME = "sample-tap-snowflake"
PLUGIN_VERSION_FILE = "resources/VERSION"
PLUGIN_CAPABILITIES = [
    "sync",
]
ACCEPTED_CONFIG = [
    "",
]
REQUIRED_CONFIG = [
    "account",
    "dbname",
    "user",
    "password",
    "warehouse",
    "tables",
]


class TooManyRecordsException(Exception):
    """Exception to raise when query returns more records than max_records"""


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
            option_set_requirements=[REQUIRED_CONFIG],
            config=config,
            state=state,
        )

    def validate_config(self) -> Tuple[List[str], List[str]]:
        """Validate configuration dictionary."""
        return super().validate_config()

    # Core plugin metadata:

    def get_available_stream_ids(self) -> List[str]:
        """Return a list of all streams (tables)."""
        result = self.query("""SELECT table_name from information_schema.tables""")
        return list(result)

    def create_stream(self, stream_id: str) -> SampleTapSnowflakeStream:
        """Return a tap stream object."""
        conn = self.get_connection()
        return SampleTapSnowflakeStream(conn, stream_id)

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

    def query(self, query, params=None, max_records=0):  # TODO: document return type
        """Run a query in snowflake."""
        result = []
        with self.connect_with_backoff() as connection:
            with connection.cursor(snowflake.connector.DictCursor) as cur:
                queries = []
                if isinstance(query, list):
                    # Run every query in one transaction if query is a list of SQL
                    queries.append("START TRANSACTION")
                    queries.extend(query)
                else:
                    queries = [query]
                for sql in queries:
                    # LOGGER.debug("SNOWFLAKE - Running query: %s", sql) # TODO: Add logger
                    cur.execute(sql, params)
                    if 0 < max_records < cur.rowcount:
                        # Raise exception if returned rows greater than max allowed records
                        raise TooManyRecordsException(
                            f"Query returned too many records. This query can return max {max_records} records"
                        )
                    if cur.rowcount > 0:
                        result = cur.fetchall()
        return result
