from __future__ import annotations

import typing as t

from sqlalchemy.engine.default import DefaultDialect

if t.TYPE_CHECKING:
    from types import ModuleType


class CustomSQLDialect(DefaultDialect):
    """Custom SQLite dialect that supports JSON."""

    name = "myrdbms"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @classmethod
    def import_dbapi(cls):
        """Import the sqlite3 DBAPI."""
        import sqlite3  # noqa: PLC0415

        return sqlite3

    @classmethod
    def dbapi(cls) -> ModuleType:  # type: ignore[override]
        """Return the DBAPI module.

        NOTE: This is a legacy method that will stop being used by SQLAlchemy at some point.
        """  # noqa: E501
        return cls.import_dbapi()
