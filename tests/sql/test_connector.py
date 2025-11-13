from __future__ import annotations

import functools
import sys
import typing as t
from decimal import Decimal
from unittest import mock

import pytest
import sqlalchemy
import sqlalchemy.engine
import sqlalchemy.exc
import sqlalchemy.schema
import sqlalchemy.types
from sqlalchemy.dialects import registry, sqlite
from sqlalchemy.engine.default import DefaultDialect

from singer_sdk.connectors import SQLConnector
from singer_sdk.connectors.sql import (
    FullyQualifiedName,
    JSONSchemaToSQL,
    SQLToJSONSchema,
)
from singer_sdk.exceptions import ConfigValidationError

if sys.version_info >= (3, 12):
    from typing import override  # noqa: ICN003
else:
    from typing_extensions import override


if t.TYPE_CHECKING:
    from pathlib import Path

    from sqlalchemy.engine import Engine


def stringify(in_dict):
    return {k: str(v) for k, v in in_dict.items()}


class MyType(sqlalchemy.TypeDecorator):
    impl = sqlalchemy.LargeBinary


class DummySQLConnector(SQLConnector):
    """Dummy SQL connector."""

    allow_column_alter = True

    @override
    @staticmethod
    def get_column_alter_ddl(
        table_name: str,
        column_name: str,
        column_type: sqlalchemy.types.TypeEngine,
    ) -> sqlalchemy.DDL:
        return sqlalchemy.DDL(
            "ALTER TABLE %(table_name)s ALTER COLUMN %(column_name)s TYPE %(column_type)s",  # noqa: E501
            {
                "table_name": table_name,
                "column_name": column_name,
                "column_type": column_type,
            },
        )


class TestConnectorSQL:  # noqa: PLR0904
    """Test the SQLConnector class."""

    @pytest.fixture
    def connector(self):
        return SQLConnector(config={"sqlalchemy_url": "sqlite:///"})

    @pytest.mark.parametrize(
        "method_name,kwargs,context,unrendered_statement,rendered_statement",
        [
            (
                "get_column_add_ddl",
                {
                    "table_name": "full.table.name",
                    "column_name": "column_name",
                    "column_type": sqlalchemy.Text(),
                },
                {
                    "table_name": "full.table.name",
                    "create_column_clause": sqlalchemy.schema.CreateColumn(
                        sqlalchemy.Column(
                            "column_name",
                            sqlalchemy.Text(),
                        ),
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
                "ALTER TABLE %(table_name)s RENAME COLUMN %(column_name)s to %(new_column_name)s",  # noqa: E501
                "ALTER TABLE full.table.name RENAME COLUMN old_name to new_name",
            ),
            (
                "get_column_alter_ddl",
                {
                    "table_name": "full.table.name",
                    "column_name": "column_name",
                    "column_type": sqlalchemy.String(),
                },
                {
                    "table_name": "full.table.name",
                    "column_name": "column_name",
                    "column_type": sqlalchemy.String(),
                },
                "ALTER TABLE %(table_name)s ALTER COLUMN %(column_name)s (%(column_type)s)",  # noqa: E501
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
                dialect=sqlite.dialect(),
                compile_kwargs={"literal_binds": True},
            ),
        )
        assert statement == rendered_statement

    def test_remove_collation_text_type(self):
        remove_collation = SQLConnector.remove_collation
        test_collation = "SQL_Latin1_General_CP1_CI_AS"
        current_type = sqlalchemy.Text(collation=test_collation)
        current_type_collation = remove_collation(current_type)
        # Check collation was set to None by the function
        assert current_type.collation is None
        # Check that we get the same collation we put in back out
        assert current_type_collation == test_collation

    def test_remove_collation_non_text_type(self):
        remove_collation = SQLConnector.remove_collation
        current_type = sqlalchemy.Integer()
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
        compatible_type = sqlalchemy.Text(collation=None)
        update_collation(compatible_type, test_collation)
        # Check collation was set to the value we put in
        assert compatible_type.collation == test_collation

    def test_update_collation_non_text_type(self):
        update_collation = SQLConnector.update_collation
        test_collation = "SQL_Latin1_General_CP1_CI_AS"
        compatible_type = sqlalchemy.Integer()
        update_collation(compatible_type, test_collation)
        # Check there is not a collation attribute
        assert not hasattr(compatible_type, "collation")
        # Check that we get the same type we put in
        assert str(compatible_type) == "INTEGER"

    def test_create_engine_returns_new_engine(self, connector):
        engine1 = connector.create_engine()
        engine2 = connector.create_engine()
        assert engine1 is not engine2

    def test_engine_creates_and_returns_cached_engine(self, connector):
        assert not connector._cached_engine
        engine1 = connector._engine
        engine2 = connector._cached_engine
        assert engine1 is engine2

    def test_deprecated_functions_warn(self, connector: SQLConnector):
        with pytest.deprecated_call():
            connector.create_sqlalchemy_engine()
        with pytest.deprecated_call():
            connector.create_sqlalchemy_connection()
        with pytest.deprecated_call():
            _ = connector.connection

    def test_connect_calls_engine(self, connector):
        with (
            mock.patch.object(SQLConnector, "_engine") as mock_engine,
            connector._connect() as _,
        ):
            mock_engine.connect.assert_called_once()

    def test_connect_calls_connect(self, connector):
        attached_engine = connector._engine
        with (
            mock.patch.object(attached_engine, "connect") as mock_conn,
            connector._connect() as _,
        ):
            mock_conn.assert_called_once()

    def test_connect_raises_on_operational_failure(self, connector):
        with (
            pytest.raises(sqlalchemy.exc.OperationalError) as _,
            connector._connect() as conn,
        ):
            conn.execute(sqlalchemy.text("SELECT * FROM fake_table"))

    def test_rename_column_uses_connect_correctly(self, connector):
        attached_engine = connector._engine
        # Ends up using the attached engine
        with mock.patch.object(attached_engine, "connect") as mock_conn:
            connector.rename_column("fake_table", "old_name", "new_name")
            mock_conn.assert_called_once()
        # Uses the _connect method
        with mock.patch.object(connector, "_connect") as mock_connect_method:
            connector.rename_column("fake_table", "old_name", "new_name")
            mock_connect_method.assert_called_once()

    def test_get_slalchemy_url_raises_if_not_in_config(self, connector):
        with pytest.raises(ConfigValidationError):
            connector.get_sqlalchemy_url({})

    def test_dialect_uses_engine(self, connector):
        attached_engine = connector._engine
        with mock.patch.object(attached_engine, "dialect") as _:
            res = connector._dialect
            assert res == attached_engine.dialect

    def test_merge_sql_types_text_current_max(self, connector: SQLConnector):
        current_type = sqlalchemy.VARCHAR(length=None)
        sql_type = sqlalchemy.VARCHAR(length=255)
        compatible_sql_type = connector.merge_sql_types([current_type, sql_type])
        # Check that the current VARCHAR(MAX) type is kept
        assert compatible_sql_type is current_type

    def test_merge_sql_types_text_current_greater_than(self, connector: SQLConnector):
        current_type = sqlalchemy.VARCHAR(length=255)
        sql_type = sqlalchemy.VARCHAR(length=64)
        compatible_sql_type = connector.merge_sql_types([current_type, sql_type])
        # Check the current greater VARCHAR(255) is kept
        assert compatible_sql_type is current_type

    def test_merge_sql_types_text_proposed_max(self, connector):
        current_type = sqlalchemy.VARCHAR(length=64)
        sql_type = sqlalchemy.VARCHAR(length=None)
        compatible_sql_type = connector.merge_sql_types([current_type, sql_type])
        # Check the current VARCHAR(64) is chosen over default VARCHAR(max)
        assert compatible_sql_type is current_type

    def test_merge_sql_types_text_current_less_than(self, connector):
        current_type = sqlalchemy.VARCHAR(length=64)
        sql_type = sqlalchemy.VARCHAR(length=255)
        compatible_sql_type = connector.merge_sql_types([current_type, sql_type])
        # Check that VARCHAR(255) is chosen over the lesser current VARCHAR(64)
        assert compatible_sql_type is sql_type

    @pytest.mark.parametrize(
        "types,expected_type",
        [
            pytest.param(
                [sqlalchemy.Integer(), sqlalchemy.Numeric()],
                sqlalchemy.Integer,
                id="integer-numeric",
            ),
            pytest.param(
                [sqlalchemy.Numeric(), sqlalchemy.Integer()],
                sqlalchemy.Numeric,
                id="numeric-integer",
            ),
            pytest.param(
                [
                    sqlalchemy.Integer(),
                    sqlalchemy.String(),
                    sqlalchemy.Numeric(),
                ],
                sqlalchemy.String,
                id="integer-string-numeric",
            ),
        ],
    )
    def test_merge_generic_sql_types(
        self,
        connector: SQLConnector,
        types: list[sqlalchemy.types.TypeEngine],
        expected_type: type[sqlalchemy.types.TypeEngine],
    ):
        merged_type = connector.merge_sql_types(types)
        assert isinstance(merged_type, expected_type)

    def test_engine_json_serialization(self, connector: SQLConnector):
        engine = connector._engine
        meta = sqlalchemy.MetaData()
        table = sqlalchemy.Table(
            "test_table",
            meta,
            sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
            sqlalchemy.Column("attrs", sqlalchemy.JSON),
        )
        meta.create_all(engine)
        with engine.connect() as conn, conn.begin():
            conn.execute(
                table.insert(),
                [
                    {"attrs": {"x": Decimal("1.0")}},
                    {"attrs": {"x": Decimal("2.0"), "y": [1, 2, 3]}},
                ],
            )
            result = conn.execute(table.select())
            assert result.fetchall() == [
                (1, {"x": Decimal("1.0")}),
                (2, {"x": Decimal("2.0"), "y": [1, 2, 3]}),
            ]

    @pytest.mark.parametrize(
        "primary_keys",
        [
            pytest.param(["col_b", "col_d", "col_a"], id="mixed-order"),
            pytest.param(["col_d", "col_c", "col_b", "col_a"], id="reverse-order"),
            pytest.param(["col_a", "col_b", "col_c", "col_d"], id="alphabetical-order"),
        ],
    )
    def test_create_empty_table_primary_key_order(
        self,
        connector: SQLConnector,
        primary_keys: list[str],
    ):
        """Test that primary key columns maintain their specified order."""
        schema = {
            "type": "object",
            "properties": {
                "col_a": {"type": "string"},
                "col_b": {"type": "integer"},
                "col_c": {"type": "string"},
                "col_d": {"type": "integer"},
            },
        }

        table_name = "test_pk_order"

        connector.create_empty_table(
            full_table_name=table_name,
            schema=schema,
            primary_keys=primary_keys,
        )

        inspector = sqlalchemy.inspect(connector._engine)
        pk_constraint = inspector.get_pk_constraint(table_name)

        assert pk_constraint["constrained_columns"] == primary_keys

        with connector._engine.connect() as conn, conn.begin():
            conn.execute(sqlalchemy.text(f"DROP TABLE {table_name}"))

    def test_create_empty_table_partial_primary_keys(self, connector: SQLConnector):
        """Test that only existing columns are included in primary key constraint."""
        schema = {
            "type": "object",
            "properties": {
                "existing_col_1": {"type": "string"},
                "existing_col_2": {"type": "integer"},
                "existing_col_3": {"type": "string"},
            },
        }

        # Primary keys include some columns not in the schema
        primary_keys = [
            "missing_col",
            "existing_col_2",
            "existing_col_1",
            "another_missing",
        ]
        table_name = "test_partial_pk"

        # Should not raise an error
        connector.create_empty_table(
            full_table_name=table_name,
            schema=schema,
            primary_keys=primary_keys,
        )

        # Inspect the created table
        inspector = sqlalchemy.inspect(connector._engine)
        pk_constraint = inspector.get_pk_constraint(table_name)

        # Should only include existing columns in the order they were specified
        expected_pk_columns = ["existing_col_2", "existing_col_1"]
        assert pk_constraint["constrained_columns"] == expected_pk_columns

        # Clean up
        with connector._engine.connect() as conn, conn.begin():
            conn.execute(sqlalchemy.text(f"DROP TABLE {table_name}"))

    def test_create_empty_table_no_primary_keys_in_schema(
        self,
        connector: SQLConnector,
    ):
        """Test behavior when no primary key columns exist in schema."""
        schema = {
            "type": "object",
            "properties": {
                "col_a": {"type": "string"},
                "col_b": {"type": "integer"},
            },
        }

        # Primary keys reference columns not in schema
        primary_keys = ["missing_col_1", "missing_col_2"]
        table_name = "test_no_pk_in_schema"

        # Should not raise an error
        connector.create_empty_table(
            full_table_name=table_name,
            schema=schema,
            primary_keys=primary_keys,
        )

        # Inspect the created table
        inspector = sqlalchemy.inspect(connector._engine)
        pk_constraint = inspector.get_pk_constraint(table_name)

        # Should have no primary key constraint
        assert pk_constraint["constrained_columns"] == []

        # Clean up
        with connector._engine.connect() as conn, conn.begin():
            conn.execute(sqlalchemy.text(f"DROP TABLE {table_name}"))

    def test_create_empty_table_schema_property_order_independence(
        self,
        connector: SQLConnector,
    ):
        """Test that primary key order is independent of schema property order."""
        # Same properties in different orders
        schema1 = {
            "type": "object",
            "properties": {
                "col_a": {"type": "string"},
                "col_b": {"type": "integer"},
                "col_c": {"type": "string"},
            },
        }

        schema2 = {
            "type": "object",
            "properties": {
                "col_c": {"type": "string"},
                "col_a": {"type": "string"},
                "col_b": {"type": "integer"},
            },
        }

        primary_keys = ["col_b", "col_c", "col_a"]

        # Create tables with different schema property orders
        table1 = "test_schema_order_1"
        table2 = "test_schema_order_2"

        connector.create_empty_table(table1, schema1, primary_keys)
        connector.create_empty_table(table2, schema2, primary_keys)

        # Both should have identical primary key order
        inspector = sqlalchemy.inspect(connector._engine)
        pk1 = inspector.get_pk_constraint(table1)["constrained_columns"]
        pk2 = inspector.get_pk_constraint(table2)["constrained_columns"]

        assert pk1 == pk2 == primary_keys

        # Clean up
        with connector._engine.connect() as conn, conn.begin():
            conn.execute(sqlalchemy.text(f"DROP TABLE {table1}"))
            conn.execute(sqlalchemy.text(f"DROP TABLE {table2}"))

    def test_sort_types(self, connector: SQLConnector):
        """Test that sort_types returns the correct order of SQL types."""
        integer_type = sqlalchemy.Integer()
        string_type = sqlalchemy.String()
        string_255_type = sqlalchemy.String(length=255)
        boolean_type = sqlalchemy.Boolean()
        datetime_type = sqlalchemy.DateTime()
        float_type = sqlalchemy.Float()

        types: list[sqlalchemy.types.TypeEngine] = [
            integer_type,
            string_type,
            string_255_type,
            boolean_type,
            datetime_type,
            float_type,
        ]
        assert connector._sort_types(types) == [
            string_255_type,
            string_type,
            datetime_type,
            float_type,
            integer_type,
            boolean_type,
        ]


class TestDummySQLConnector:
    @pytest.fixture
    def connector(self):
        return DummySQLConnector(config={"sqlalchemy_url": "sqlite:///"})

    def test_create_schema(self, connector: DummySQLConnector):
        # CREATE SCHEMA is not supported by SQLite, so we mock
        with (
            mock.patch.object(connector, "_connect") as mock_connect,
            mock.patch.object(mock_connect, "begin"),
            mock.patch.object(mock_connect, "execute"),
        ):
            connector.create_schema("test_schema")
            mock_connect.assert_called_once()
            mock_connect.return_value.__enter__.return_value.begin.assert_called_once()
            mock_connect.return_value.__enter__.return_value.execute.assert_called_once()

    def test_column_rename(self, connector: DummySQLConnector):
        engine = connector._engine
        meta = sqlalchemy.MetaData()
        _ = sqlalchemy.Table(
            "test_table",
            meta,
            sqlalchemy.Column("id", sqlalchemy.Integer),
            sqlalchemy.Column("old_name", sqlalchemy.String),
        )
        meta.create_all(engine)

        connector.rename_column("test_table", "old_name", "new_name")

        with engine.connect() as conn:
            result = conn.execute(sqlalchemy.text("SELECT * FROM test_table"))
            assert result.keys() == ["id", "new_name"]

    def test_adapt_column_type(self, connector: DummySQLConnector):
        engine = connector._engine
        meta = sqlalchemy.MetaData()
        _ = sqlalchemy.Table(
            "test_table",
            meta,
            sqlalchemy.Column("id", sqlalchemy.Integer),
            sqlalchemy.Column("name", sqlalchemy.Integer),
        )
        meta.create_all(engine)

        # Changing the column type is not supported by SQLite, so we mock
        with (
            mock.patch.object(connector, "_connect") as mock_connect,
            mock.patch.object(mock_connect, "begin"),
            mock.patch.object(mock_connect, "execute"),
        ):
            connector._adapt_column_type("test_table", "name", sqlalchemy.String())
            mock_connect.assert_called_once()
            mock_begin = mock_connect.return_value.__enter__.return_value.begin
            mock_begin.assert_called_once()
            mock_execute = mock_connect.return_value.__enter__.return_value.execute
            ddl = mock_execute.call_args[0][0]
            assert isinstance(ddl, sqlalchemy.DDL)
            assert (
                str(ddl.compile())
                == "ALTER TABLE test_table ALTER COLUMN name TYPE VARCHAR"
            )

    @pytest.mark.parametrize(
        "exclude_schemas,expected_streams",
        [
            ([], 1),
            (["main"], 0),
        ],
    )
    def test_discover_catalog_entries_exclude_schemas(
        self,
        connector: DummySQLConnector,
        exclude_schemas: list[str],
        expected_streams: int,
    ):
        with connector._engine.connect() as conn, conn.begin():
            conn.execute(
                sqlalchemy.text(
                    "CREATE TABLE test_table (id INTEGER PRIMARY KEY, name STRING)",
                )
            )
        entries = connector.discover_catalog_entries(
            exclude_schemas=exclude_schemas,
            reflect_indices=False,
        )
        assert len(entries) == expected_streams


def test_adapter_without_json_serde():
    registry.register(
        "myrdbms",
        "samples.sample_custom_sql_adapter.connector",
        "CustomSQLDialect",
    )

    class CustomConnector(SQLConnector):
        def create_engine(self) -> Engine:
            return super().create_engine()

    connector = CustomConnector(config={"sqlalchemy_url": "myrdbms:///"})
    connector.create_engine()


def test_fully_qualified_name():
    fqn = FullyQualifiedName(table="my_table")
    assert fqn == "my_table"

    fqn = FullyQualifiedName(schema="my_schema", table="my_table")
    assert fqn == "my_schema.my_table"

    fqn = FullyQualifiedName(
        database="my_catalog",
        schema="my_schema",
        table="my_table",
    )
    assert fqn == "my_catalog.my_schema.my_table"


def test_fully_qualified_name_with_quoting():
    class QuotedFullyQualifiedName(FullyQualifiedName):
        def __init__(self, *, dialect: sqlalchemy.engine.Dialect, **kwargs: t.Any):
            self.dialect = dialect
            super().__init__(**kwargs)

        def prepare_part(self, part: str) -> str:
            return self.dialect.identifier_preparer.quote(part)

    dialect = DefaultDialect()

    fqn = QuotedFullyQualifiedName(table="order", schema="public", dialect=dialect)
    assert fqn == 'public."order"'


def test_fully_qualified_name_empty_error():
    with pytest.raises(ValueError, match="Could not generate fully qualified name"):
        FullyQualifiedName()


@pytest.mark.parametrize(
    "sql_type, expected_jsonschema_type",
    [
        pytest.param(sqlalchemy.VARCHAR(), {"type": ["string"]}, id="varchar"),
        pytest.param(
            sqlalchemy.VARCHAR(length=127),
            {"type": ["string"], "maxLength": 127},
            id="varchar-length",
        ),
        pytest.param(sqlalchemy.TEXT(), {"type": ["string"]}, id="text"),
        pytest.param(sqlalchemy.INTEGER(), {"type": ["integer"]}, id="integer"),
        pytest.param(sqlalchemy.BOOLEAN(), {"type": ["boolean"]}, id="boolean"),
        pytest.param(sqlalchemy.DECIMAL(), {"type": ["number"]}, id="decimal"),
        pytest.param(sqlalchemy.FLOAT(), {"type": ["number"]}, id="float"),
        pytest.param(sqlalchemy.REAL(), {"type": ["number"]}, id="real"),
        pytest.param(sqlalchemy.NUMERIC(), {"type": ["number"]}, id="numeric"),
        pytest.param(
            sqlalchemy.DATE(),
            {"type": ["string"], "format": "date"},
            id="date",
        ),
        pytest.param(
            sqlalchemy.DATETIME(),
            {"type": ["string"], "format": "date-time"},
            id="datetime",
        ),
        pytest.param(
            sqlalchemy.TIMESTAMP(),
            {"type": ["string"], "format": "date-time"},
            id="timestamp",
        ),
        pytest.param(
            sqlalchemy.TIME(),
            {"type": ["string"], "format": "time"},
            id="time",
        ),
        pytest.param(
            sqlalchemy.BLOB(),
            {"type": ["string"]},
            id="unknown",
        ),
    ],
)
def test_sql_to_json_schema_map(
    sql_type: sqlalchemy.types.TypeEngine,
    expected_jsonschema_type: dict,
):
    m = SQLToJSONSchema()
    assert m.to_jsonschema(sql_type) == expected_jsonschema_type


def test_custom_type_to_jsonschema():
    class MyMap(SQLToJSONSchema):
        @functools.singledispatchmethod
        def to_jsonschema(self, column_type: sqlalchemy.types.TypeEngine):
            return super().to_jsonschema(column_type)

        @to_jsonschema.register
        def custom_number_to_jsonschema(self, column_type: sqlalchemy.Numeric) -> dict:
            """Custom number to JSON schema.

            For example, a scale of 4 translates to a multipleOf 0.0001.
            """
            schema = {"type": ["number"]}
            if column_type.scale is not None:
                schema["multipleOf"] = 10**-column_type.scale
            return schema

        @to_jsonschema.register(MyType)
        def my_type_to_jsonschema(self, column_type: MyType) -> dict:
            return {
                "type": ["string"],
                "contentEncoding": "base64",
                "x-impl": column_type.__class__.__name__,
            }

    m = MyMap()

    assert m.to_jsonschema(MyType()) == {
        "type": ["string"],
        "contentEncoding": "base64",
        "x-impl": "MyType",
    }
    assert m.to_jsonschema(sqlalchemy.NUMERIC()) == {"type": ["number"]}
    assert m.to_jsonschema(sqlalchemy.NUMERIC(scale=2)) == {
        "type": ["number"],
        "multipleOf": 0.01,
    }
    assert m.to_jsonschema(sqlalchemy.BOOLEAN()) == {"type": ["boolean"]}


def test_numeric_to_singer_decimal():
    converter = SQLToJSONSchema(use_singer_decimal=True)
    assert converter.to_jsonschema(sqlalchemy.NUMERIC()) == {
        "type": ["string"],
        "format": "singer.decimal",
    }


class TestJSONSchemaToSQL:  # noqa: PLR0904
    @pytest.fixture
    def json_schema_to_sql(self) -> JSONSchemaToSQL:
        return JSONSchemaToSQL(max_varchar_length=65_535)

    def test_register_jsonschema_type_handler(
        self,
        json_schema_to_sql: JSONSchemaToSQL,
    ):
        json_schema_to_sql.register_type_handler("my-type", sqlalchemy.LargeBinary)
        result = json_schema_to_sql.to_sql_type({"type": "my-type"})
        assert isinstance(result, sqlalchemy.LargeBinary)

    def test_register_jsonschema_format_handler(
        self,
        json_schema_to_sql: JSONSchemaToSQL,
    ):
        json_schema_to_sql.register_format_handler("my-format", sqlalchemy.LargeBinary)
        result = json_schema_to_sql.to_sql_type(
            {
                "type": "string",
                "format": "my-format",
            }
        )
        assert isinstance(result, sqlalchemy.LargeBinary)

    def test_string(self, json_schema_to_sql: JSONSchemaToSQL):
        jsonschema_type = {"type": ["string", "null"]}
        result = json_schema_to_sql.to_sql_type(jsonschema_type)
        assert isinstance(result, sqlalchemy.VARCHAR)
        assert result.length is None

    @pytest.mark.parametrize(
        "jsonschema_type,expected_length",
        [
            pytest.param(
                {"type": ["string", "null"], "maxLength": 10},
                10,
                id="max-length",
            ),
            pytest.param(
                {"type": ["string", "null"], "maxLength": 1_000_000},
                65_535,
                id="max-length-clamped",
            ),
        ],
    )
    def test_string_max_length(
        self,
        json_schema_to_sql: JSONSchemaToSQL,
        jsonschema_type: dict,
        expected_length: int,
    ):
        result = json_schema_to_sql.to_sql_type(jsonschema_type)
        assert isinstance(result, sqlalchemy.VARCHAR)
        assert result.length == expected_length

    def test_integer(self, json_schema_to_sql: JSONSchemaToSQL):
        jsonschema_type = {"type": ["integer", "null"]}
        result = json_schema_to_sql.to_sql_type(jsonschema_type)
        assert isinstance(result, sqlalchemy.INTEGER)

    def test_number(self, json_schema_to_sql: JSONSchemaToSQL):
        jsonschema_type = {"type": ["number", "null"]}
        result = json_schema_to_sql.to_sql_type(jsonschema_type)
        assert isinstance(result, sqlalchemy.DECIMAL)

    def test_boolean(self, json_schema_to_sql: JSONSchemaToSQL):
        jsonschema_type = {"type": ["boolean", "null"]}
        result = json_schema_to_sql.to_sql_type(jsonschema_type)
        assert isinstance(result, sqlalchemy.BOOLEAN)

    def test_object(self, json_schema_to_sql: JSONSchemaToSQL):
        jsonschema_type = {"type": "object", "properties": {}}
        result = json_schema_to_sql.to_sql_type(jsonschema_type)
        assert isinstance(result, sqlalchemy.VARCHAR)

    def test_array(self, json_schema_to_sql: JSONSchemaToSQL):
        jsonschema_type = {"type": "array"}
        result = json_schema_to_sql.to_sql_type(jsonschema_type)
        assert isinstance(result, sqlalchemy.VARCHAR)

    def test_array_items(self, json_schema_to_sql: JSONSchemaToSQL):
        jsonschema_type = {"type": "array", "items": {"type": "string"}}
        result = json_schema_to_sql.to_sql_type(jsonschema_type)
        assert isinstance(result, sqlalchemy.VARCHAR)

    def test_date(self, json_schema_to_sql: JSONSchemaToSQL):
        jsonschema_type = {"format": "date", "type": ["string", "null"]}
        result = json_schema_to_sql.to_sql_type(jsonschema_type)
        assert isinstance(result, sqlalchemy.DATE)

    def test_time(self, json_schema_to_sql: JSONSchemaToSQL):
        jsonschema_type = {"format": "time", "type": ["string", "null"]}
        result = json_schema_to_sql.to_sql_type(jsonschema_type)
        assert isinstance(result, sqlalchemy.TIME)

    def test_uuid(self, json_schema_to_sql: JSONSchemaToSQL):
        jsonschema_type = {"format": "uuid", "type": ["string", "null"]}
        result = json_schema_to_sql.to_sql_type(jsonschema_type)
        assert isinstance(result, sqlalchemy.UUID)

    def test_datetime(self, json_schema_to_sql: JSONSchemaToSQL):
        jsonschema_type = {"format": "date-time", "type": ["string", "null"]}
        result = json_schema_to_sql.to_sql_type(jsonschema_type)
        assert isinstance(result, sqlalchemy.DATETIME)

    def test_anyof_datetime(self, json_schema_to_sql: JSONSchemaToSQL):
        jsonschema_type = {
            "anyOf": [
                {"type": "string", "format": "date-time"},
                {"type": "null"},
            ],
        }
        result = json_schema_to_sql.to_sql_type(jsonschema_type)
        assert isinstance(result, sqlalchemy.DATETIME)

    def test_anyof_integer(self, json_schema_to_sql: JSONSchemaToSQL):
        jsonschema_type = {
            "anyOf": [
                {"type": "null"},
                {"type": "integer"},
            ],
        }
        result = json_schema_to_sql.to_sql_type(jsonschema_type)
        assert isinstance(result, sqlalchemy.INTEGER)

    def test_anyof_json(self):
        json_schema_to_sql = JSONSchemaToSQL()
        json_schema_to_sql.register_type_handler("object", sqlalchemy.JSON)
        jsonschema_type = {
            "anyOf": [
                {"type": "null"},
                {"type": "object"},
            ],
        }
        result = json_schema_to_sql.to_sql_type(jsonschema_type)
        assert isinstance(result, sqlalchemy.JSON)

    def test_anyof_unknown(self, json_schema_to_sql: JSONSchemaToSQL):
        jsonschema_type = {
            "anyOf": [
                {"type": "null"},
                {"type": "unknown"},
            ],
        }
        result = json_schema_to_sql.to_sql_type(jsonschema_type)
        assert isinstance(result, sqlalchemy.VARCHAR)

    @pytest.mark.parametrize(
        "jsonschema_type,expected_type",
        [
            pytest.param(
                {"type": ["unknown", "null"]},
                sqlalchemy.VARCHAR,
                id="unknown",
            ),
            pytest.param(
                {"type": ["array", "object", "boolean", "null"]},
                sqlalchemy.VARCHAR,
                id="array-first",
            ),
            pytest.param(
                {"type": ["boolean", "array", "object", "null"]},
                sqlalchemy.VARCHAR,
                id="boolean-first",
            ),
        ],
    )
    def test_multiple_types(
        self,
        json_schema_to_sql: JSONSchemaToSQL,
        jsonschema_type: dict,
        expected_type: type[sqlalchemy.types.TypeEngine],
    ):
        result = json_schema_to_sql.to_sql_type(jsonschema_type)
        assert isinstance(result, expected_type)

    def test_multiple_types_custom_handler(self):
        class CustomJSONSchemaToSQL(JSONSchemaToSQL):
            def handle_multiple_types(
                self,
                types: list[str],
            ) -> sqlalchemy.types.TypeEngine:
                if "object" in types or "array" in types:
                    return sqlalchemy.JSON()
                return super().handle_multiple_types(types)

        json_schema_to_sql = CustomJSONSchemaToSQL()
        jsonschema_type = {"type": ["object", "array", "string", "null"]}
        result = json_schema_to_sql.to_sql_type(jsonschema_type)
        assert isinstance(result, sqlalchemy.JSON)

        jsonschema_type = {"type": ["string", "number", "null"]}
        result = json_schema_to_sql.to_sql_type(jsonschema_type)
        assert isinstance(result, sqlalchemy.VARCHAR)

    def test_unknown_type(self, json_schema_to_sql: JSONSchemaToSQL):
        jsonschema_type = {"cannot": "compute"}
        result = json_schema_to_sql.to_sql_type(jsonschema_type)
        assert isinstance(result, sqlalchemy.VARCHAR)

    def test_unknown_format(self, json_schema_to_sql: JSONSchemaToSQL):
        jsonschema_type = {"type": "string", "format": "unknown"}
        result = json_schema_to_sql.to_sql_type(jsonschema_type)
        assert isinstance(result, sqlalchemy.VARCHAR)

    def test_custom_fallback(self):
        json_schema_to_sql = JSONSchemaToSQL(max_varchar_length=None)
        json_schema_to_sql.fallback_type = sqlalchemy.CHAR
        jsonschema_type = {"cannot": "compute"}
        result = json_schema_to_sql.to_sql_type(jsonschema_type)
        assert isinstance(result, sqlalchemy.CHAR)

    def test_custom_handle_raw_string(self):
        class CustomJSONSchemaToSQL(JSONSchemaToSQL):
            def handle_raw_string(self, schema):
                if schema.get("contentMediaType") == "image/png":
                    return sqlalchemy.LargeBinary()

                return super().handle_raw_string(schema)

        json_schema_to_sql = CustomJSONSchemaToSQL(max_varchar_length=None)

        vanilla = {"type": ["string"]}
        result = json_schema_to_sql.to_sql_type(vanilla)
        assert isinstance(result, sqlalchemy.VARCHAR)

        non_image_type = {
            "type": "string",
            "contentMediaType": "text/html",
        }
        result = json_schema_to_sql.to_sql_type(non_image_type)
        assert isinstance(result, sqlalchemy.VARCHAR)

        image_type = {
            "type": "string",
            "contentEncoding": "base64",
            "contentMediaType": "image/png",
        }
        result = json_schema_to_sql.to_sql_type(image_type)
        assert isinstance(result, sqlalchemy.LargeBinary)

    def test_singer_decimal(self):
        json_schema_to_sql = JSONSchemaToSQL()
        jsonschema_type = {"type": ["string"], "format": "x-singer.decimal"}
        result = json_schema_to_sql.to_sql_type(jsonschema_type)
        assert isinstance(result, sqlalchemy.DECIMAL)

    def test_singer_decimal_with_precision_scale(self):
        json_schema_to_sql = JSONSchemaToSQL()
        precision, scale = 12, 3
        jsonschema_type = {
            "type": ["string"],
            "format": "x-singer.decimal",
            "precision": precision,
            "scale": scale,
        }
        result = json_schema_to_sql.to_sql_type(jsonschema_type)
        assert isinstance(result, sqlalchemy.DECIMAL)
        assert result.precision == precision
        assert result.scale == scale

    def test_handle_singer_decimal_missing_precision(self):
        json_schema_to_sql = JSONSchemaToSQL(max_varchar_length=None)
        schema = {
            "type": ["string"],
            "format": "x-singer.decimal",
            # 'precision' is missing
            "scale": 2,
        }
        result = json_schema_to_sql.to_sql_type(schema)
        assert isinstance(result, sqlalchemy.DECIMAL)
        assert result.precision is None
        assert result.scale == 2

    def test_handle_singer_decimal_missing_scale(self):
        json_schema_to_sql = JSONSchemaToSQL(max_varchar_length=None)
        schema = {
            "type": ["string"],
            "format": "x-singer.decimal",
            "precision": 10,
            # 'scale' is missing
        }
        result = json_schema_to_sql.to_sql_type(schema)
        assert isinstance(result, sqlalchemy.DECIMAL)
        assert result.precision == 10
        assert result.scale is None

    def test_annotation_sql_datatype(self):
        json_schema_to_sql = JSONSchemaToSQL()
        json_schema_to_sql.register_sql_datatype_handler("json", sqlalchemy.JSON)
        jsonschema_type = {"type": ["string"], "x-sql-datatype": "json"}
        result = json_schema_to_sql.to_sql_type(jsonschema_type)
        assert isinstance(result, sqlalchemy.JSON)

        unknown_type = {"type": ["string"], "x-sql-datatype": "unknown"}
        with pytest.warns(
            UserWarning,
            match="This target does not support the x-sql-datatype",
        ):
            result = json_schema_to_sql.to_sql_type(unknown_type)

        assert isinstance(result, sqlalchemy.VARCHAR)


def test_bench_discovery(benchmark, tmp_path: Path):
    def _discover_catalog(connector):
        connector.discover_catalog_entries()

    number_of_tables = 250
    number_of_views = 250
    number_of_columns = 10
    db_path = tmp_path / "foo.db"
    engine = sqlalchemy.create_engine(f"sqlite:///{db_path}")

    columns_fragment = ",".join(f"col_{i} VARCHAR" for i in range(number_of_columns))

    # Seed a large number of tables
    table_ddl = f"""
    CREATE TABLE table_{{n}} (
        id INTEGER NOT NULL,
        {columns_fragment},
        PRIMARY KEY (id)
    );
    """

    # Seed a large number of views
    view_ddl = """
    CREATE VIEW view_{n} AS
        SELECT * FROM table_{n};
    """

    with engine.connect() as conn:
        for i in range(number_of_tables):
            conn.execute(sqlalchemy.text(table_ddl.format(n=i)))

        for i in range(number_of_views):
            conn.execute(sqlalchemy.text(view_ddl.format(n=i)))

    connector = SQLConnector(config={"sqlalchemy_url": f"sqlite:///{db_path}"})

    benchmark(_discover_catalog, connector)
