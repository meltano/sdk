"""Sample tap stream test for tap-snowflake."""

from typing import Dict, Iterable, List, Tuple, Union, cast
from snowflake import connector

from tap_base.streams import DatabaseStreamBase
from tap_base.exceptions import TooManyRecordsException


from tap_base.tests.sample_tap_snowflake.snowflake_config import PLUGIN_NAME

DEFAULT_BATCH_SIZE = 10000


class SampleTapSnowflakeStream(DatabaseStreamBase):
    """Sample tap test for snowflake."""

    tap_name = PLUGIN_NAME

    @classmethod
    def scan_primary_keys(cls, config) -> Dict[Tuple[str, str, str], List[str]]:
        """Snowflake does not support primary keys. Return empty result."""
        return {}

    @classmethod
    def sql_query(
        cls,
        sql: Union[str, List[str]],
        config,
        params=None,
        max_records=0,
        dict_results=True,
    ) -> Union[Iterable[dict], Iterable[List]]:
        """Run a query in snowflake."""
        # connection = cls.connect_with_retries()
        connection = cls.open_connection(config=config)
        with connection.cursor(connector.DictCursor) as cur:
            queries = []
            if isinstance(sql, list):
                # Run every query in one transaction if query is a list of SQL
                queries.append("START TRANSACTION")
                queries.extend(sql)
            else:
                queries = [sql]
            num_queries = len(queries)
            for i, sql in enumerate(queries, 1):
                # if i == num_queries:
                #     cls.logger.info("Executing asynchronous Snowflake query: %s", sql)
                #     cur.execute_async(sql, params)
                #     cls.logger.into(f"Submitted asynchronous QueryID {cur.sfqid}")
                #     # status = connection.get_query_status_throw_if_error(cur.sfqid)
                #     cur.get_results_from_sfqid(cur.sfqid)
                # else:
                cls.logger.info("Executing synchronous Snowflake query: %s", sql)
                cur.execute(sql, params)
                if max_records and cur.rowcount > max_records:
                    # Raise exception if num rows greater than max allowed records
                    raise TooManyRecordsException(
                        "Query returned too many records. "
                        f"This query can return max {max_records} records."
                    )
                result_batch = cur.fetchmany(DEFAULT_BATCH_SIZE)
                while len(result_batch) > 0:
                    for result in result_batch:
                        if dict_results:
                            yield result
                        else:
                            yield result.values()
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
