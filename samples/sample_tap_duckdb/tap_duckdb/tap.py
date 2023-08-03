import typing as t
from textwrap import dedent

import sqlalchemy as sa

from singer_sdk import SQLConnector, SQLTap
from singer_sdk import typing as th
from singer_sdk.helpers._batch import BaseBatchFileEncoding, BatchConfig
from singer_sdk.streams import SQLStream


class DuckDBBatchError(Exception):
    """DuckDB batch error."""
    pass


class DuckDBConnector(SQLConnector):
    def get_sqlalchemy_url(self, config) -> str:
        """Return a SQLAlchemy URL."""
        return f"duckdb:///{config['database']}"


class DuckDBStream(SQLStream):
    connector_class = DuckDBConnector

    def get_batches(self, batch_config: BatchConfig, context = None):
        root = batch_config.storage.root
        prefix = batch_config.storage.prefix or ""
        filepath = "out/out.ndjson"
        table_name = self.fully_qualified_name
        files = []

        file_format = batch_config.encoding.format
        compression = batch_config.encoding.compression

        options = []
        if file_format == "jsonl":
            options.append("FORMAT JSON")
        else:
            msg = f"Unsupported file format: {file_format}"
            raise DuckDBBatchError(msg)

        if compression is None or compression == "none":
            pass
        elif compression == "gzip":
            options.append("COMPRESSION GZIP")
        else:
            msg = f"Unsupported compression: {compression}"
            raise DuckDBBatchError(msg)

        copy_options = ",".join(options)
        query = sa.text(
            dedent(
                f"COPY (SELECT * FROM {table_name}) TO '{filepath}' "  # noqa: S608
                f"({copy_options})",
            ),
        )
        query = query.execution_options(autocommit=True)

        with self.connector._connect() as conn:
            conn.execute(query)

        files.append(filepath)

        yield (batch_config.encoding, files)


class TapDuckDB(SQLTap):
    name = "tap-duckdb"
    default_stream_class = DuckDBStream

    config_jsonschema = th.PropertiesList(
        th.Property(
            "database",
            th.StringType,
            required=True,
            description="Path to the DuckDB database file.",
            default="data.duckdb",
        ),
    ).to_dict()


if __name__ == "__main__":
    TapDuckDB.cli()
