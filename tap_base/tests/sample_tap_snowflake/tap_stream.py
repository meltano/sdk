"""Sample tap stream test for tap-snowflake."""

from tap_base.tap_stream_base import TapStreamBase
from typing import List, Union
from snowflake import connector

from tap_base.streams import DatabaseStreamBase
from tap_base.exceptions import TooManyRecordsException


class SampleTapSnowflakeStream(DatabaseStreamBase):
    """Sample tap test for snowflake."""

    THREE_PART_NAMES: bool = True
    MAX_CONNECT_ATTEMPTS = 5

    def __init__(self, tap_stream_id: str, schema: dict, properties: dict = None):
        """Initialize stream class."""
        pass

    def query(self, query: Union[str, List[str]], params=None, max_records=0):
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
                    if max_records and cur.rowcount > max_records:
                        # Raise exception if num rows greater than max allowed records
                        raise TooManyRecordsException(
                            "Query returned too many records. "
                            f"This query can return max {max_records} records."
                        )
                    if cur.rowcount > 0:
                        result = cur.fetchall()
        return result

    def open_connection(self) -> connector.SnowflakeConnection:
        """Connect to snowflake database."""
        self._conn = connector.connect(
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
