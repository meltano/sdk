"""Sample tap test for tap-snowflake."""

from typing import List, Union
from snowflake.connector.connection import SnowflakeConnection
import snowflake.connector

from tap_base import DatabaseConnectionBase
from tap_base.exceptions import TooManyRecordsException


class SampleSnowflakeConnection(DatabaseConnectionBase):
    """Snowflake Tap Connection Class."""

    THREE_PART_NAMES: bool = True
    MAX_CONNECT_ATTEMPTS = 5

    _conn: SnowflakeConnection

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

    def open_connection(self) -> SnowflakeConnection:
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
