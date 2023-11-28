from __future__ import annotations

import typing as t
import uuid
from textwrap import dedent

import sqlalchemy as sa

from singer_sdk import SQLConnector, SQLTap
from singer_sdk import typing as th
from singer_sdk.exceptions import (
    UnsupportedBatchCompressionError,
    UnsupportedBatchFormatError,
)
from singer_sdk.helpers._batch import BatchFileFormat, CSVEncoding, JSONLinesEncoding
from singer_sdk.streams import SQLStream

if t.TYPE_CHECKING:
    from singer_sdk.helpers._batch import BatchConfig


class DuckDBConnector(SQLConnector):
    def get_sqlalchemy_url(self, config) -> str:
        """Return a SQLAlchemy URL."""
        return f"duckdb:///{config['database']}"


class DuckDBStream(SQLStream):
    connector_class = DuckDBConnector

    def get_jsonl_copy_options(self, encoding: JSONLinesEncoding, filename: str):
        options = ["FORMAT JSON"]
        if encoding.compression is None or encoding.compression == "none":
            filename = f"{filename}.jsonl"
        elif encoding.compression == "gzip":
            options.append("COMPRESSION GZIP")
            filename = f"{filename}.jsonl.gz"
        else:
            raise UnsupportedBatchCompressionError(encoding.compression)

        return options, filename

    def get_csv_copy_options(self, encoding: CSVEncoding, filename: str):
        options = ["FORMAT CSV", f"DELIMITER '{encoding.delimiter}'"]
        if encoding.header:
            options.append("HEADER")

        if encoding.compression is None or encoding.compression == "none":
            filename = f"{filename}.csv"
        elif encoding.compression == "gzip":
            options.append("COMPRESSION GZIP")
            filename = f"{filename}.csv.gz"
        else:
            raise UnsupportedBatchCompressionError(encoding.compression)

        return options, filename

    def get_batches(self, batch_config: BatchConfig, context=None):  # noqa: ARG002
        prefix = batch_config.storage.prefix or ""
        sync_id = f"{self.tap_name}--{self.name}-{uuid.uuid4()}"
        filename = f"{prefix}{sync_id}"
        file_format = batch_config.encoding.format

        if file_format == BatchFileFormat.JSONL:
            options, filename = self.get_jsonl_copy_options(
                batch_config.encoding,
                filename,
            )
        elif file_format == BatchFileFormat.CSV:
            options, filename = self.get_csv_copy_options(
                batch_config.encoding,
                filename,
            )
        else:
            raise UnsupportedBatchFormatError(file_format)

        table_name = self.fully_qualified_name
        filepath = f"{batch_config.storage.root}/{filename}"
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

        files = [filepath]
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
