"""Sample tap stream test for tap-snowflake."""

from typing import Iterable, List, Optional, Tuple, Union
from snowflake import connector

from singer_sdk.helpers.classproperty import classproperty
from singer_sdk.streams import DatabaseStream


DEFAULT_BATCH_SIZE = 10000


class SampleTapSnowflakeStream(DatabaseStream):
    """Sample tap test for snowflake."""

    @classproperty
    # @classmethod
    def primary_key_scan_sql(cls) -> Optional[str]:
        """Snowflake does not support primary keys. Return empty result."""
        return None

    @classmethod
    def execute_query(cls, sql: Union[str, List[str]], config) -> Iterable[dict]:
        """Run a query in snowflake."""
        connection = cls.open_connection(config=config)
        with connection.cursor(connector.DictCursor) as cur:
            queries = []
            if isinstance(sql, list):
                # Run every query in one transaction if query is a list of SQL
                queries.append("START TRANSACTION")
                queries.extend(sql)
            else:
                queries = [sql]
            for sql in queries:
                cls.logger.info("Executing synchronous Snowflake query: %s", sql)
                cur.execute(sql)
                result_batch = cur.fetchmany(DEFAULT_BATCH_SIZE)
                while len(result_batch) > 0:
                    for result in result_batch:
                        yield result
                    result_batch = cur.fetchmany(DEFAULT_BATCH_SIZE)
            cur.close()

    @classmethod
    def open_connection(cls, config) -> connector.SnowflakeConnection:
        """Connect to snowflake database."""
        acct = config["account"]
        db = config["dbname"]
        wh = config["warehouse"]
        usr = config["user"]
        cls.logger.info(
            f"Attempting to connect to Snowflake '{db}' database on "
            f"account '{acct}' instance with warehouse '{wh}' and user '{usr}'."
        )
        conn = connector.connect(
            account=acct,
            database=db,
            warehouse=wh,
            user=usr,
            password=config["password"],
            insecure_mode=config.get("insecure_mode", False)
            # Use insecure mode to avoid "Failed to get OCSP response" warnings
            # insecure_mode=True
        )
        return conn
