"""SQL-related functionality for the Singer SDK."""

from __future__ import annotations

__lazy_modules__ = {
    "singer_sdk.sql.connector",
    "singer_sdk.sql.sink",
    "singer_sdk.sql.stream",
    "singer_sdk.sql.tap",
    "singer_sdk.sql.target",
}

from singer_sdk.sql.connector import SQLConnector
from singer_sdk.sql.sink import SQLSink
from singer_sdk.sql.stream import SQLStream
from singer_sdk.sql.tap import SQLTap
from singer_sdk.sql.target import SQLTarget

__all__ = [
    "SQLConnector",
    "SQLSink",
    "SQLStream",
    "SQLTap",
    "SQLTarget",
]
