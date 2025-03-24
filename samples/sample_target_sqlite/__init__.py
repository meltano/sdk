"""A sample implementation for SQLite."""

from __future__ import annotations

import datetime
import sqlite3
import typing as t

from singer_sdk import SQLConnector, SQLSink, SQLTarget
from singer_sdk import typing as th
from singer_sdk.contrib.msgspec import MsgSpecReader

DB_PATH_CONFIG = "path_to_db"


def adapt_date_iso(val):
    """Adapt datetime.date to ISO 8601 date."""
    return val.isoformat()


def adapt_datetime_iso(val):
    """Adapt datetime.datetime to timezone-naive ISO 8601 date."""
    return val.isoformat()


def adapt_datetime_epoch(val):
    """Adapt datetime.datetime to Unix timestamp."""
    return int(val.timestamp())


sqlite3.register_adapter(datetime.date, adapt_date_iso)
sqlite3.register_adapter(datetime.datetime, adapt_datetime_iso)
sqlite3.register_adapter(datetime.datetime, adapt_datetime_epoch)


def convert_date(val):
    """Convert ISO 8601 date to datetime.date object."""
    return datetime.date.fromisoformat(val.decode())


def convert_datetime(val):
    """Convert ISO 8601 datetime to datetime.datetime object."""
    return datetime.datetime.fromisoformat(val.decode())


def convert_timestamp(val):
    """Convert Unix epoch timestamp to datetime.datetime object."""
    return datetime.datetime.fromtimestamp(int(val), tz=datetime.timezone.utc)


sqlite3.register_converter("date", convert_date)
sqlite3.register_converter("datetime", convert_datetime)
sqlite3.register_converter("timestamp", convert_timestamp)


class SQLiteConnector(SQLConnector):
    """The connector for SQLite.

    This class handles all DDL and type conversions.
    """

    allow_temp_tables = False
    allow_column_alter = False
    allow_merge_upsert = True
    allow_overwrite: bool = True

    def get_sqlalchemy_url(self, config: dict[str, t.Any]) -> str:  # noqa: PLR6301
        """Generates a SQLAlchemy URL for SQLite."""
        return f"sqlite:///{config[DB_PATH_CONFIG]}"


class SQLiteSink(SQLSink[SQLiteConnector]):
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

    message_reader_class = MsgSpecReader

    name = "target-sqlite-sample"
    default_sink_class = SQLiteSink
    max_parallelism = 1

    config_jsonschema = th.PropertiesList(
        th.Property(
            DB_PATH_CONFIG,
            th.StringType,
            title="Database Path",
            description="The path to your SQLite database file(s).",
            required=True,
        ),
    ).to_dict()


__all__ = ["SQLiteConnector", "SQLiteSink", "SQLiteTarget"]
