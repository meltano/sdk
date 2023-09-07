"""A sample implementation for SQLite."""

from __future__ import annotations

import typing as t

from singer_sdk import SQLConnector, SQLSink, SQLTarget
from singer_sdk import typing as th

DB_PATH_CONFIG = "path_to_db"


class SQLiteConnector(SQLConnector):
    """The connector for SQLite.

    This class handles all DDL and type conversions.
    """

    allow_temp_tables = False
    allow_column_alter = False
    allow_merge_upsert = True
    allow_overwrite: bool = True

    def get_sqlalchemy_url(self, config: dict[str, t.Any]) -> str:
        """Generates a SQLAlchemy URL for SQLite."""
        return f"sqlite:///{config[DB_PATH_CONFIG]}"


class SQLiteSink(SQLSink):
    """The Sink class for SQLite.

    This class allows developers to optionally override `get_records()` and other
    stream methods in order to improve performance beyond the default SQLAlchemy-based
    interface.

    DDL and type conversion operations are delegated to the connector logic specified
    in `connector_class` or by overriding the `connector` object.
    """

    connector_class = SQLiteConnector


class SQLiteTarget(SQLTarget):
    """The Tap class for SQLite."""

    name = "target-sqlite-sample"
    default_sink_class = SQLiteSink
    max_parallelism = 1

    config_jsonschema = th.PropertiesList(
        th.Property(
            DB_PATH_CONFIG,
            th.StringType,
            description="The path to your SQLite database file(s).",
        ),
    ).to_dict()


__all__ = ["SQLiteTarget", "SQLiteConnector", "SQLiteSink"]
