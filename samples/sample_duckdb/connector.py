"""DuckDB connector."""

from __future__ import annotations

import sys

import sqlalchemy as sa

from singer_sdk.connectors import SQLConnector

if sys.version_info < (3, 12):
    from typing_extensions import override
else:
    from typing import override  # noqa: ICN003


class DuckDBConnector(SQLConnector):
    """DuckDB connector."""

    allow_column_alter = True

    @override
    @staticmethod
    def get_column_alter_ddl(
        table_name: str,
        column_name: str,
        column_type: sa.types.TypeEngine,
    ) -> sa.DDL:
        return sa.DDL(
            "ALTER TABLE %(table_name)s ALTER COLUMN %(column_name)s TYPE %(column_type)s",  # noqa: E501
            {
                "table_name": table_name,
                "column_name": column_name,
                "column_type": column_type,
            },
        )
