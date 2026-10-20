"""Custom SQL dialect."""

from __future__ import annotations

import sys
import typing as t

from sqlalchemy.engine.default import DefaultDialect

if sys.version_info >= (3, 12):
    from typing import override  # noqa: ICN003
else:
    from typing_extensions import override

if t.TYPE_CHECKING:
    from sqlalchemy.engine.interfaces import DBAPIModule


class CustomSQLDialect(DefaultDialect):
    """Custom SQLite dialect that supports JSON."""

    name = "myrdbms"

    @classmethod
    @override
    def import_dbapi(cls) -> DBAPIModule:
        from sqlite3 import dbapi2 as sqlite  # noqa: PLC0415

        return t.cast("DBAPIModule", sqlite)
