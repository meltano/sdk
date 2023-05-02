"""A sample implementation for SQLite."""

from __future__ import annotations

import typing as t

from singer_sdk import SQLConnector, SQLStream, SQLTap
from singer_sdk import typing as th

DB_PATH_CONFIG = "path_to_db"


class SQLiteConnector(SQLConnector):
    """The connector for SQLite.

    This class handles all DDL and type conversions.
    """

    def get_sqlalchemy_url(self, config: dict[str, t.Any]) -> str:
        """Generates a SQLAlchemy URL for SQLite."""
        return f"sqlite:///{config[DB_PATH_CONFIG]}"


class SQLiteStream(SQLStream):
    """The Stream class for SQLite.

    This class allows developers to optionally override `get_records()` and other
    stream methods in order to improve performance beyond the default SQLAlchemy-based
    interface.

    DDL and type conversion operations are delegated to the connector logic specified
    in `connector_class` or by overriding the `connector` object.
    """

    connector_class = SQLiteConnector


class SQLiteTap(SQLTap):
    """The Tap class for SQLite."""

    name = "tap-sqlite-sample"
    default_stream_class = SQLiteStream
    config_jsonschema = th.PropertiesList(
        th.Property(
            DB_PATH_CONFIG,
            th.StringType,
            description="The path to your SQLite database file(s).",
            examples=["./path/to/my.db", "/absolute/path/to/my.db"],
        ),
    ).to_dict()


__all__ = ["SQLiteTap", "SQLiteConnector", "SQLiteStream"]
