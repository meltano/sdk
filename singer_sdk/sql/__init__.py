"""SQL-related functionality for the Singer SDK."""

from __future__ import annotations

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
