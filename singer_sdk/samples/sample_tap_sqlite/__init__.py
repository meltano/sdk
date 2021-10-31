"""A sample implementation for SQLite."""

from typing import Any, Dict
from singer_sdk import SQLStream, SQLTap, SQLConnector
from singer_sdk import typing as th

DB_PATH = "path_to_db"


class SQLiteConnector(SQLConnector):
    """The connector for SQLite."""

    def get_sqlalchemy_url(self, config: Dict[str, Any]) -> str:
        """Generates a SQLAlchemy URL for SQLite."""
        return f"sqlite:///{config[DB_PATH]}"


class SQLiteStream(SQLStream):
    """The Stream class for SQLite."""

    connector_class = SQLiteConnector


class SQLiteTap(SQLTap):
    """The Tap class for SQLite."""

    name = "tap-sqlite-sample"
    default_stream_class = SQLiteStream
    config_jsonschema = th.PropertiesList(
        th.Property(
            DB_PATH,
            th.StringType,
            description="The path to your SQLite database file(s).",
        )
    ).to_dict()
