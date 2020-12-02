"""Sample tap test for tap-pardot."""

from typing import List, Union
from pardot.connector.connection import PardotConnection
import pardot.connector

from tap_base import DatabaseConnectionBase


class TooManyRecordsException(Exception):
    """Exception to raise when query returns more records than max_records."""


class SampleTapPardotConnection(DatabaseConnectionBase):
    """Pardot Tap Connection Class."""

    THREE_PART_NAMES: bool = True

    _conn: PardotConnection

    def query(self, query: Union[str, List[str]], params=None, max_records=0):
        """Run a query in pardot."""
        result = []
        with self.connect_with_backoff() as connection:
            with connection.cursor(pardot.connector.DictCursor) as cur:
                queries = []
                if isinstance(query, list):
                    # Run every query in one transaction if query is a list of SQL
                    queries.append("START TRANSACTION")
                    queries.extend(query)
                else:
                    queries = [query]
                for sql in queries:
                    # LOGGER.debug("PARDOT - Running query: %s", sql) # TODO: Add logger
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

    def open_connection(self) -> PardotConnection:
        """Connect to pardot database."""
        self._conn = pardot.connector.connect(
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
