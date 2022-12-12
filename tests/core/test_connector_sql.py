from unittest.mock import patch

import pytest
import sqlalchemy
from sqlalchemy.dialects import sqlite

from singer_sdk.connectors import SQLConnector


def stringify(in_dict):
    return {k: str(v) for k, v in in_dict.items()}


class TestConnectorSQL:
    """Test the SQLConnector class."""

    @pytest.fixture()
    def connector(self):
        return SQLConnector()

    @pytest.mark.parametrize(
        "method_name,kwargs,context,unrendered_statement,rendered_statement",
        [
            (
                "get_column_add_ddl",
                {
                    "table_name": "full.table.name",
                    "column_name": "column_name",
                    "column_type": sqlalchemy.types.Text(),
                },
                {
                    "table_name": "full.table.name",
                    "create_column_clause": sqlalchemy.schema.CreateColumn(
                        sqlalchemy.Column(
                            "column_name",
                            sqlalchemy.types.Text(),
                        )
                    ),
                },
                "ALTER TABLE %(table_name)s ADD COLUMN %(create_column_clause)s",
                "ALTER TABLE full.table.name ADD COLUMN column_name TEXT",
            ),
            (
                "get_column_rename_ddl",
                {
                    "table_name": "full.table.name",
                    "column_name": "old_name",
                    "new_column_name": "new_name",
                },
                {
                    "table_name": "full.table.name",
                    "column_name": "old_name",
                    "new_column_name": "new_name",
                },
                "ALTER TABLE %(table_name)s RENAME COLUMN %(column_name)s to %(new_column_name)s",
                "ALTER TABLE full.table.name RENAME COLUMN old_name to new_name",
            ),
            (
                "get_column_alter_ddl",
                {
                    "table_name": "full.table.name",
                    "column_name": "column_name",
                    "column_type": sqlalchemy.types.String(),
                },
                {
                    "table_name": "full.table.name",
                    "column_name": "column_name",
                    "column_type": sqlalchemy.types.String(),
                },
                "ALTER TABLE %(table_name)s ALTER COLUMN %(column_name)s (%(column_type)s)",
                "ALTER TABLE full.table.name ALTER COLUMN column_name (VARCHAR)",
            ),
        ],
    )
    def test_get_column_ddl(
        self,
        connector,
        method_name,
        kwargs,
        context,
        unrendered_statement,
        rendered_statement,
    ):
        method = getattr(connector, method_name)
        column_ddl = method(**kwargs)

        assert stringify(column_ddl.context) == stringify(context)
        assert column_ddl.statement == unrendered_statement

        statement = str(
            column_ddl.compile(
                dialect=sqlite.dialect(), compile_kwargs={"literal_binds": True}
            )
        )
        assert statement == rendered_statement

    @pytest.mark.parametrize(
        "method_name,kwargs,expected_value",
        [
            (
                "get_unconformed_column_name",
                {
                    "full_table_name": "test_table",
                    "column_name": "id",
                },
                "Id",
            ),
            (
                "get_unconformed_column_name",
                {
                    "full_table_name": "test_table",
                    "column_name": "test_column",
                },
                "TestColumn",
            ),
            (
                "get_unconformed_column_name",
                {
                    "full_table_name": "test_table",
                    "column_name": "test_id",
                },
                None,
            ),
        ],
    )
    @patch(
        "singer_sdk.connectors.SQLConnector.get_table_columns",
        return_value={
            "Id": "Column('Id', INTEGER(), table=None, nullable=False)",
            "TestColumn": "Column('TestColumn',  VARCHAR(), table=None)",
        },
        autospec=True,
    )
    def test_get_unconformed_column_name(
        self, patch, connector, method_name, kwargs, expected_value
    ):
        method = getattr(connector, method_name)
        method_value = method(**kwargs)
        assert method_value == expected_value

    @pytest.mark.parametrize(
        "method_name,kwargs,expected_value",
        [
            (
                "unconformed_column_exists",
                {
                    "full_table_name": "test_table",
                    "column_name": "id",
                },
                True,
            ),
            (
                "unconformed_column_exists",
                {
                    "full_table_name": "test_table",
                    "column_name": "test_column",
                },
                True,
            ),
            (
                "unconformed_column_exists",
                {
                    "full_table_name": "test_table",
                    "column_name": "TestColumn",
                },
                False,
            ),
        ],
    )
    @patch(
        "singer_sdk.connectors.SQLConnector.get_table_columns",
        return_value={
            "Id": "Column('Id', INTEGER(), table=None, nullable=False)",
            "TestColumn": "Column('TestColumn',  VARCHAR(), table=None)",
        },
        autospec=True,
    )
    def test_unconformed_column_exists(
        self, patch, connector, method_name, kwargs, expected_value
    ):
        method = getattr(connector, method_name)
        method_value = method(**kwargs)
        assert method_value == expected_value
