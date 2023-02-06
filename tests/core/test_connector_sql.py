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

    def test_remove_collation_text_type(self):
        remove_collation = SQLConnector.remove_collation
        test_collation = "SQL_Latin1_General_CP1_CI_AS"
        current_type = sqlalchemy.types.Text(collation=test_collation)
        current_type_collation = remove_collation(current_type)
        # Check collation was set to None by the function
        assert current_type.collation is None
        # Check that we get the same collation we put in back out
        assert current_type_collation == test_collation

    def test_remove_collation_non_text_type(self):
        remove_collation = SQLConnector.remove_collation
        current_type = sqlalchemy.types.Integer()
        current_type_collation = remove_collation(current_type)
        # Check there is not a collation attribute
        assert not hasattr(current_type, "collation")
        # Check that we get the same type we put in
        assert str(current_type) == "INTEGER"
        # Check that this variable is missing
        assert current_type_collation is None

    def test_update_collation_text_type(self):
        update_collation = SQLConnector.update_collation
        test_collation = "SQL_Latin1_General_CP1_CI_AS"
        compatible_type = sqlalchemy.types.Text(collation=None)
        update_collation(compatible_type, test_collation)
        # Check collation was set to the value we put in
        assert compatible_type.collation == test_collation

    def test_update_collation_non_text_type(self):
        update_collation = SQLConnector.update_collation
        test_collation = "SQL_Latin1_General_CP1_CI_AS"
        compatible_type = sqlalchemy.types.Integer()
        update_collation(compatible_type, test_collation)
        # Check there is not a collation attribute
        assert not hasattr(compatible_type, "collation")
        # Check that we get the same type we put in
        assert str(compatible_type) == "INTEGER"

    def test_merge_sql_types_text_current_max(self):
        cls = SQLConnector()
        current_type = sqlalchemy.types.VARCHAR(length=None)
        sql_type = sqlalchemy.types.VARCHAR(length=255)
        compatible_sql_type = cls.merge_sql_types([current_type, sql_type])
        # Check that the current VARCHAR(MAX) type is kept
        assert compatible_sql_type is current_type

    def test_merge_sql_types_text_current_greater_than(self):
        cls = SQLConnector()
        current_type = sqlalchemy.types.VARCHAR(length=255)
        sql_type = sqlalchemy.types.VARCHAR(length=64)
        compatible_sql_type = cls.merge_sql_types([current_type, sql_type])
        # Check the current greater VARCHAR(255) is kept
        assert compatible_sql_type is current_type

    def test_merge_sql_types_text_proposed_max(self):
        cls = SQLConnector()
        current_type = sqlalchemy.types.VARCHAR(length=64)
        sql_type = sqlalchemy.types.VARCHAR(length=None)
        compatible_sql_type = cls.merge_sql_types([current_type, sql_type])
        # Check the current VARCHAR(64) is chosen over default varcahr(max)
        assert compatible_sql_type is current_type

    def test_merge_sql_types_text_current_less_than(self):
        cls = SQLConnector()
        current_type = sqlalchemy.types.VARCHAR(length=64)
        sql_type = sqlalchemy.types.VARCHAR(length=255)
        compatible_sql_type = cls.merge_sql_types([current_type, sql_type])
        # Check that VARCHAR(255) is chosen over the lesser current VARCHAR(64)
        assert compatible_sql_type is sql_type
