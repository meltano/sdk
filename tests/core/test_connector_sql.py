from __future__ import annotations

import functools
import typing as t
from decimal import Decimal
from unittest import mock

import pytest
import sqlalchemy as sa
from sqlalchemy.dialects import registry, sqlite
from sqlalchemy.engine.default import DefaultDialect

from samples.sample_duckdb import DuckDBConnector
from singer_sdk.connectors import SQLConnector
from singer_sdk.connectors.sql import (
    FullyQualifiedName,
    JSONSchemaToSQL,
    SQLToJSONSchema,
)
from singer_sdk.exceptions import ConfigValidationError

if t.TYPE_CHECKING:
    from pathlib import Path

    from sqlalchemy.engine import Engine


def stringify(in_dict):
    return {k: str(v) for k, v in in_dict.items()}


class MyType(sa.types.TypeDecorator):
    impl = sa.types.LargeBinary


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
                    "column_type": sa.types.Text(),
                },
                {
                    "table_name": "full.table.name",
                    "create_column_clause": sa.schema.CreateColumn(
                        sa.Column(
                            "column_name",
                            sa.types.Text(),
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
                    "column_type": sa.types.String(),
                },
                {
                    "table_name": "full.table.name",
                    "column_name": "column_name",
                    "column_type": sa.types.String(),
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
        current_type = sa.types.Text(collation=test_collation)
        current_type_collation = remove_collation(current_type)
        # Check collation was set to None by the function
        assert current_type.collation is None
        # Check that we get the same collation we put in back out
        assert current_type_collation == test_collation

    def test_remove_collation_non_text_type(self):
        remove_collation = SQLConnector.remove_collation
        current_type = sa.types.Integer()
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
        compatible_type = sa.types.Text(collation=None)
        update_collation(compatible_type, test_collation)
        # Check collation was set to the value we put in
        assert compatible_type.collation == test_collation

    def test_update_collation_non_text_type(self):
        update_collation = SQLConnector.update_collation
        test_collation = "SQL_Latin1_General_CP1_CI_AS"
        compatible_type = sa.types.Integer()
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
            pytest.raises(sa.exc.OperationalError) as _,
            connector._connect() as conn,
        ):
            conn.execute(sa.text("SELECT * FROM fake_table"))

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
        current_type = sa.types.VARCHAR(length=None)
        sql_type = sa.types.VARCHAR(length=255)
        compatible_sql_type = connector.merge_sql_types([current_type, sql_type])
        # Check that the current VARCHAR(MAX) type is kept
        assert compatible_sql_type is current_type

    def test_merge_sql_types_text_current_greater_than(self, connector: SQLConnector):
        current_type = sa.types.VARCHAR(length=255)
        sql_type = sa.types.VARCHAR(length=64)
        compatible_sql_type = connector.merge_sql_types([current_type, sql_type])
        # Check the current greater VARCHAR(255) is kept
        assert compatible_sql_type is current_type

    def test_merge_sql_types_text_proposed_max(self, connector):
        current_type = sa.types.VARCHAR(length=64)
        sql_type = sa.types.VARCHAR(length=None)
        compatible_sql_type = connector.merge_sql_types([current_type, sql_type])
        # Check the current VARCHAR(64) is chosen over default VARCHAR(max)
        assert compatible_sql_type is current_type

    def test_merge_sql_types_text_current_less_than(self, connector):
        current_type = sa.types.VARCHAR(length=64)
        sql_type = sa.types.VARCHAR(length=255)
        compatible_sql_type = connector.merge_sql_types([current_type, sql_type])
        # Check that VARCHAR(255) is chosen over the lesser current VARCHAR(64)
        assert compatible_sql_type is sql_type

    @pytest.mark.parametrize(
        "types,expected_type",
        [
            pytest.param(
                [sa.types.Integer(), sa.types.Numeric()],
                sa.types.Integer,
                id="integer-numeric",
            ),
            pytest.param(
                [sa.types.Numeric(), sa.types.Integer()],
                sa.types.Numeric,
                id="numeric-integer",
            ),
            pytest.param(
                [
                    sa.types.Integer(),
                    sa.types.String(),
                    sa.types.Numeric(),
                ],
                sa.types.String,
                id="integer-string-numeric",
            ),
        ],
    )
    def test_merge_generic_sql_types(
        self,
        connector: SQLConnector,
        types: list[sa.types.TypeEngine],
        expected_type: type[sa.types.TypeEngine],
    ):
        merged_type = connector.merge_sql_types(types)
        assert isinstance(merged_type, expected_type)

    def test_engine_json_serialization(self, connector: SQLConnector):
        engine = connector._engine
        meta = sa.MetaData()
        table = sa.Table(
            "test_table",
            meta,
            sa.Column("id", sa.Integer, primary_key=True),
            sa.Column("attrs", sa.JSON),
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


class TestDuckDBConnector:
    @pytest.fixture
    def connector(self):
        return DuckDBConnector(config={"sqlalchemy_url": "duckdb:///"})

    def test_create_schema(self, connector: DuckDBConnector):
        engine = connector._engine
        connector.create_schema("test_schema")
        inspector = sa.inspect(engine)
        assert "memory.test_schema" in inspector.get_schema_names()

    def test_column_rename(self, connector: DuckDBConnector):
        engine = connector._engine
        meta = sa.MetaData()
        _ = sa.Table(
            "test_table",
            meta,
            sa.Column("id", sa.Integer),
            sa.Column("old_name", sa.String),
        )
        meta.create_all(engine)

        connector.rename_column("test_table", "old_name", "new_name")

        with engine.connect() as conn:
            result = conn.execute(sa.text("SELECT * FROM test_table"))
            assert result.keys() == ["id", "new_name"]

    def test_adapt_column_type(self, connector: DuckDBConnector):
        connector.allow_column_alter = True
        engine = connector._engine
        meta = sa.MetaData()
        _ = sa.Table(
            "test_table",
            meta,
            sa.Column("id", sa.Integer),
            sa.Column("name", sa.Integer),
        )
        meta.create_all(engine)

        connector._adapt_column_type("test_table", "name", sa.types.String())

        with engine.connect() as conn:
            result = conn.execute(sa.text("SELECT * FROM test_table"))
            assert result.keys() == ["id", "name"]
            assert result.cursor.description[1][1] == "STRING"

    @pytest.mark.parametrize(
        "exclude_schemas,expected_streams",
        [
            ([], 1),
            (["memory.my_schema"], 0),
        ],
    )
    def test_discover_catalog_entries_exclude_schemas(
        self,
        connector: DuckDBConnector,
        exclude_schemas: list[str],
        expected_streams: int,
    ):
        with connector._engine.connect() as conn, conn.begin():
            conn.execute(sa.text("CREATE SCHEMA my_schema"))
            conn.execute(
                sa.text(
                    "CREATE TABLE my_schema.test_table (id INTEGER PRIMARY KEY, name STRING)",  # noqa: E501
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
        def __init__(self, *, dialect: sa.engine.Dialect, **kwargs: t.Any):
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
        pytest.param(sa.types.VARCHAR(), {"type": ["string"]}, id="varchar"),
        pytest.param(
            sa.types.VARCHAR(length=127),
            {"type": ["string"], "maxLength": 127},
            id="varchar-length",
        ),
        pytest.param(sa.types.TEXT(), {"type": ["string"]}, id="text"),
        pytest.param(sa.types.INTEGER(), {"type": ["integer"]}, id="integer"),
        pytest.param(sa.types.BOOLEAN(), {"type": ["boolean"]}, id="boolean"),
        pytest.param(sa.types.DECIMAL(), {"type": ["number"]}, id="decimal"),
        pytest.param(sa.types.FLOAT(), {"type": ["number"]}, id="float"),
        pytest.param(sa.types.REAL(), {"type": ["number"]}, id="real"),
        pytest.param(sa.types.NUMERIC(), {"type": ["number"]}, id="numeric"),
        pytest.param(
            sa.types.DATE(),
            {"type": ["string"], "format": "date"},
            id="date",
        ),
        pytest.param(
            sa.types.DATETIME(),
            {"type": ["string"], "format": "date-time"},
            id="datetime",
        ),
        pytest.param(
            sa.types.TIMESTAMP(),
            {"type": ["string"], "format": "date-time"},
            id="timestamp",
        ),
        pytest.param(
            sa.types.TIME(),
            {"type": ["string"], "format": "time"},
            id="time",
        ),
        pytest.param(
            sa.types.BLOB(),
            {"type": ["string"]},
            id="unknown",
        ),
    ],
)
def test_sql_to_json_schema_map(
    sql_type: sa.types.TypeEngine,
    expected_jsonschema_type: dict,
):
    m = SQLToJSONSchema()
    assert m.to_jsonschema(sql_type) == expected_jsonschema_type


def test_custom_type_to_jsonschema():
    class MyMap(SQLToJSONSchema):
        @functools.singledispatchmethod
        def to_jsonschema(self, column_type: sa.types.TypeEngine):
            return super().to_jsonschema(column_type)

        @to_jsonschema.register
        def custom_number_to_jsonschema(self, column_type: sa.types.Numeric) -> dict:
            """Custom number to JSON schema.

            For example, a scale of 4 translates to a multipleOf 0.0001.
            """
            return {"type": ["number"], "multipleOf": 10**-column_type.scale}

        @to_jsonschema.register(MyType)
        def my_type_to_jsonschema(self, column_type) -> dict:  # noqa: ARG002
            return {"type": ["string"], "contentEncoding": "base64"}

    m = MyMap()

    assert m.to_jsonschema(MyType()) == {
        "type": ["string"],
        "contentEncoding": "base64",
    }
    assert m.to_jsonschema(sa.types.NUMERIC(scale=2)) == {
        "type": ["number"],
        "multipleOf": 0.01,
    }
    assert m.to_jsonschema(sa.types.BOOLEAN()) == {"type": ["boolean"]}


def test_numeric_to_singer_decimal():
    converter = SQLToJSONSchema(use_singer_decimal=True)
    assert converter.to_jsonschema(sa.types.NUMERIC()) == {
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
        json_schema_to_sql.register_type_handler("my-type", sa.types.LargeBinary)
        result = json_schema_to_sql.to_sql_type({"type": "my-type"})
        assert isinstance(result, sa.types.LargeBinary)

    def test_register_jsonschema_format_handler(
        self,
        json_schema_to_sql: JSONSchemaToSQL,
    ):
        json_schema_to_sql.register_format_handler("my-format", sa.types.LargeBinary)
        result = json_schema_to_sql.to_sql_type(
            {
                "type": "string",
                "format": "my-format",
            }
        )
        assert isinstance(result, sa.types.LargeBinary)

    def test_string(self, json_schema_to_sql: JSONSchemaToSQL):
        jsonschema_type = {"type": ["string", "null"]}
        result = json_schema_to_sql.to_sql_type(jsonschema_type)
        assert isinstance(result, sa.types.VARCHAR)
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
        assert isinstance(result, sa.types.VARCHAR)
        assert result.length == expected_length

    def test_integer(self, json_schema_to_sql: JSONSchemaToSQL):
        jsonschema_type = {"type": ["integer", "null"]}
        result = json_schema_to_sql.to_sql_type(jsonschema_type)
        assert isinstance(result, sa.types.INTEGER)

    def test_number(self, json_schema_to_sql: JSONSchemaToSQL):
        jsonschema_type = {"type": ["number", "null"]}
        result = json_schema_to_sql.to_sql_type(jsonschema_type)
        assert isinstance(result, sa.types.DECIMAL)

    def test_boolean(self, json_schema_to_sql: JSONSchemaToSQL):
        jsonschema_type = {"type": ["boolean", "null"]}
        result = json_schema_to_sql.to_sql_type(jsonschema_type)
        assert isinstance(result, sa.types.BOOLEAN)

    def test_object(self, json_schema_to_sql: JSONSchemaToSQL):
        jsonschema_type = {"type": "object", "properties": {}}
        result = json_schema_to_sql.to_sql_type(jsonschema_type)
        assert isinstance(result, sa.types.VARCHAR)

    def test_array(self, json_schema_to_sql: JSONSchemaToSQL):
        jsonschema_type = {"type": "array"}
        result = json_schema_to_sql.to_sql_type(jsonschema_type)
        assert isinstance(result, sa.types.VARCHAR)

    def test_array_items(self, json_schema_to_sql: JSONSchemaToSQL):
        jsonschema_type = {"type": "array", "items": {"type": "string"}}
        result = json_schema_to_sql.to_sql_type(jsonschema_type)
        assert isinstance(result, sa.types.VARCHAR)

    def test_date(self, json_schema_to_sql: JSONSchemaToSQL):
        jsonschema_type = {"format": "date", "type": ["string", "null"]}
        result = json_schema_to_sql.to_sql_type(jsonschema_type)
        assert isinstance(result, sa.types.DATE)

    def test_time(self, json_schema_to_sql: JSONSchemaToSQL):
        jsonschema_type = {"format": "time", "type": ["string", "null"]}
        result = json_schema_to_sql.to_sql_type(jsonschema_type)
        assert isinstance(result, sa.types.TIME)

    def test_uuid(self, json_schema_to_sql: JSONSchemaToSQL):
        jsonschema_type = {"format": "uuid", "type": ["string", "null"]}
        result = json_schema_to_sql.to_sql_type(jsonschema_type)
        assert isinstance(result, sa.types.UUID)

    def test_datetime(self, json_schema_to_sql: JSONSchemaToSQL):
        jsonschema_type = {"format": "date-time", "type": ["string", "null"]}
        result = json_schema_to_sql.to_sql_type(jsonschema_type)
        assert isinstance(result, sa.types.DATETIME)

    def test_anyof_datetime(self, json_schema_to_sql: JSONSchemaToSQL):
        jsonschema_type = {
            "anyOf": [
                {"type": "string", "format": "date-time"},
                {"type": "null"},
            ],
        }
        result = json_schema_to_sql.to_sql_type(jsonschema_type)
        assert isinstance(result, sa.types.DATETIME)

    def test_anyof_integer(self, json_schema_to_sql: JSONSchemaToSQL):
        jsonschema_type = {
            "anyOf": [
                {"type": "null"},
                {"type": "integer"},
            ],
        }
        result = json_schema_to_sql.to_sql_type(jsonschema_type)
        assert isinstance(result, sa.types.INTEGER)

    def test_anyof_unknown(self, json_schema_to_sql: JSONSchemaToSQL):
        jsonschema_type = {
            "anyOf": [
                {"type": "null"},
                {"type": "unknown"},
            ],
        }
        result = json_schema_to_sql.to_sql_type(jsonschema_type)
        assert isinstance(result, sa.types.VARCHAR)

    @pytest.mark.parametrize(
        "jsonschema_type,expected_type",
        [
            pytest.param(
                {"type": ["array", "object", "boolean", "null"]},
                sa.types.VARCHAR,
                id="array-first",
            ),
            pytest.param(
                {"type": ["boolean", "array", "object", "null"]},
                sa.types.VARCHAR,
                id="boolean-first",
            ),
        ],
    )
    def test_complex(
        self,
        json_schema_to_sql: JSONSchemaToSQL,
        jsonschema_type: dict,
        expected_type: type[sa.types.TypeEngine],
    ):
        result = json_schema_to_sql.to_sql_type(jsonschema_type)
        assert isinstance(result, expected_type)

    def test_unknown_type(self, json_schema_to_sql: JSONSchemaToSQL):
        jsonschema_type = {"cannot": "compute"}
        result = json_schema_to_sql.to_sql_type(jsonschema_type)
        assert isinstance(result, sa.types.VARCHAR)

    def test_unknown_format(self, json_schema_to_sql: JSONSchemaToSQL):
        jsonschema_type = {"type": "string", "format": "unknown"}
        result = json_schema_to_sql.to_sql_type(jsonschema_type)
        assert isinstance(result, sa.types.VARCHAR)

    def test_custom_fallback(self):
        json_schema_to_sql = JSONSchemaToSQL(max_varchar_length=None)
        json_schema_to_sql.fallback_type = sa.types.CHAR
        jsonschema_type = {"cannot": "compute"}
        result = json_schema_to_sql.to_sql_type(jsonschema_type)
        assert isinstance(result, sa.types.CHAR)

    def test_custom_handle_raw_string(self):
        class CustomJSONSchemaToSQL(JSONSchemaToSQL):
            def handle_raw_string(self, schema):
                if schema.get("contentMediaType") == "image/png":
                    return sa.types.LargeBinary()

                return super().handle_raw_string(schema)

        json_schema_to_sql = CustomJSONSchemaToSQL(max_varchar_length=None)

        vanilla = {"type": ["string"]}
        result = json_schema_to_sql.to_sql_type(vanilla)
        assert isinstance(result, sa.types.VARCHAR)

        non_image_type = {
            "type": "string",
            "contentMediaType": "text/html",
        }
        result = json_schema_to_sql.to_sql_type(non_image_type)
        assert isinstance(result, sa.types.VARCHAR)

        image_type = {
            "type": "string",
            "contentEncoding": "base64",
            "contentMediaType": "image/png",
        }
        result = json_schema_to_sql.to_sql_type(image_type)
        assert isinstance(result, sa.types.LargeBinary)

    def test_singer_decimal(self):
        json_schema_to_sql = JSONSchemaToSQL()
        jsonschema_type = {"type": ["string"], "format": "x-singer.decimal"}
        result = json_schema_to_sql.to_sql_type(jsonschema_type)
        assert isinstance(result, sa.types.DECIMAL)

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
        assert isinstance(result, sa.types.DECIMAL)
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
        assert isinstance(result, sa.types.DECIMAL)
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
        assert isinstance(result, sa.types.DECIMAL)
        assert result.precision == 10
        assert result.scale is None

    def test_annotation_sql_datatype(self):
        json_schema_to_sql = JSONSchemaToSQL()
        json_schema_to_sql.register_sql_datatype_handler("json", sa.types.JSON)
        jsonschema_type = {"type": ["string"], "x-sql-datatype": "json"}
        result = json_schema_to_sql.to_sql_type(jsonschema_type)
        assert isinstance(result, sa.types.JSON)

        unknown_type = {"type": ["string"], "x-sql-datatype": "unknown"}
        with pytest.warns(
            UserWarning,
            match="This target does not support the x-sql-datatype",
        ):
            result = json_schema_to_sql.to_sql_type(unknown_type)

        assert isinstance(result, sa.types.VARCHAR)


def test_bench_discovery(benchmark, tmp_path: Path):
    def _discover_catalog(connector):
        connector.discover_catalog_entries()

    number_of_tables = 250
    number_of_views = 250
    number_of_columns = 10
    db_path = tmp_path / "foo.db"
    engine = sa.create_engine(f"sqlite:///{db_path}")

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
            conn.execute(sa.text(table_ddl.format(n=i)))

        for i in range(number_of_views):
            conn.execute(sa.text(view_ddl.format(n=i)))

    connector = SQLConnector(config={"sqlalchemy_url": f"sqlite:///{db_path}"})

    benchmark(_discover_catalog, connector)
