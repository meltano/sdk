"""DuckDB connector."""

from __future__ import annotations

import sqlalchemy as sa
from typing_extensions import override

from singer_sdk.connectors import SQLConnector


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
