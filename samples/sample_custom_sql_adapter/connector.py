from __future__ import annotations

from sqlalchemy.engine.default import DefaultDialect


class CustomSQLDialect(DefaultDialect):
    """Custom SQLite dialect that supports JSON."""

    name = "myrdbms"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @classmethod
    def import_dbapi(cls):
        """Import the sqlite3 DBAPI."""
        import sqlite3

        return sqlite3
