from __future__ import annotations

import typing as t
from decimal import Decimal
from unittest import mock

import pytest
import sqlalchemy as sa
from sqlalchemy.dialects import registry, sqlite
from sqlalchemy.engine.default import DefaultDialect

from samples.sample_duckdb import DuckDBConnector
from singer_sdk.connectors import SQLConnector
from singer_sdk.connectors.sql import FullyQualifiedName
from singer_sdk.exceptions import ConfigValidationError

if t.TYPE_CHECKING:
    from sqlalchemy.engine import Engine


def stringify(in_dict):
    return {k: str(v) for k, v in in_dict.items()}


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

    def test_deprecated_functions_warn(self, connector):
        with pytest.deprecated_call():
            connector.create_sqlalchemy_engine()
        with pytest.deprecated_call():
            connector.create_sqlalchemy_connection()
        with pytest.deprecated_call():
            _ = connector.connection

    def test_connect_calls_engine(self, connector):
        with mock.patch.object(
            SQLConnector,
            "_engine",
        ) as mock_engine, connector._connect() as _:
            mock_engine.connect.assert_called_once()

    def test_connect_calls_connect(self, connector):
        attached_engine = connector._engine
        with mock.patch.object(
            attached_engine,
            "connect",
        ) as mock_conn, connector._connect() as _:
            mock_conn.assert_called_once()

    def test_connect_raises_on_operational_failure(self, connector):
        with pytest.raises(
            sa.exc.OperationalError,
        ) as _, connector._connect() as conn:
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
            result = conn.execute(
                sa.text("SELECT * FROM test_table"),
            )
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
            result = conn.execute(
                sa.text("SELECT * FROM test_table"),
            )
            assert result.keys() == ["id", "name"]
            assert result.cursor.description[1][1] == "STRING"


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
