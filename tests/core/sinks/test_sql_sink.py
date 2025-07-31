from __future__ import annotations

import sys
import typing as t

import pytest
import sqlalchemy
import sqlalchemy.types

from singer_sdk.connectors import SQLConnector
from singer_sdk.sinks.sql import SQLSink
from singer_sdk.target_base import SQLTarget

if sys.version_info >= (3, 12):
    from typing import override  # noqa: ICN003
else:
    from typing_extensions import override


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


class DummySQLSink(SQLSink):
    connector_class = DummySQLConnector


class DummySQLTarget(SQLTarget):
    """Dummy SQL target class."""

    name = "dummy-sql-target"
    config_jsonschema: t.ClassVar[dict] = {"type": "object", "properties": {}}
    default_sink_class = DummySQLSink


class TestSQLSink:
    @pytest.fixture
    def target(self) -> DummySQLTarget:
        return DummySQLTarget(config={"sqlalchemy_url": "sqlite:///"})

    @pytest.fixture
    def schema(self) -> dict:
        return {
            "properties": {
                "id": {
                    "type": ["string", "null"],
                },
                "col_ts": {
                    "format": "date-time",
                    "type": ["string", "null"],
                },
                "table": {
                    "type": ["string", "null"],
                },
            },
        }

    @pytest.fixture
    def sink(self, target: DummySQLTarget, schema: dict) -> DummySQLSink:
        return DummySQLSink(
            target,
            stream_name="foo",
            schema=schema,
            key_properties=["id"],
        )

    def test_generate_insert_statement(self, sink: DummySQLSink, schema: dict):
        """Test that the insert statement is generated correctly."""
        stmt = sink.generate_insert_statement("foo", schema=schema)
        assert isinstance(stmt, sqlalchemy.Insert)
        assert stmt.table.name == "foo"
        assert stmt.table.columns.keys() == ["id", "col_ts", "table"]

        # Rendered SQL should look like:
        assert str(stmt) == (
            'INSERT INTO foo (id, col_ts, "table") VALUES (:id, :col_ts, :table)'
        )

    @pytest.mark.parametrize(
        "stream_name, default_target_schema, expected_table_name, expected_schema_name",
        [
            pytest.param(
                "foo",  # stream_name
                None,  # default_target_schema
                "foo",  # expected_table_name
                None,  # expected_schema_name
                id="no-default-schema",
            ),
            pytest.param(
                "foo-bar",  # stream_name
                None,  # default_target_schema
                "bar",  # expected_table_name
                "foo",  # expected_schema_name
                id="no-default-schema-2-part",
            ),
            pytest.param(
                "foo-bar-baz",  # stream_name
                None,  # default_target_schema
                "baz",  # expected_table_name
                "bar",  # expected_schema_name
                id="no-default-schema-3-part",
            ),
            pytest.param(
                "foo",  # stream_name
                "test",  # default_target_schema
                "foo",  # expected_table_name
                "test",  # expected_schema_name
                id="default-schema",
            ),
            pytest.param(
                "foo-bar",  # stream_name
                "test",  # default_target_schema
                "bar",  # expected_table_name
                "test",  # expected_schema_name
                id="default-schema-2-part",
            ),
            pytest.param(
                "foo-bar-baz",  # stream_name
                "test",  # default_target_schema
                "baz",  # expected_table_name
                "test",  # expected_schema_name
                id="default-schema-3-part",
            ),
        ],
    )
    def test_table_name(
        self,
        schema: dict,
        stream_name: str,
        default_target_schema: str | None,
        expected_table_name: str,
        expected_schema_name: str,
    ):
        target = DummySQLTarget(
            config={
                "sqlalchemy_url": "sqlite:///",
                "default_target_schema": default_target_schema,
            },
        )

        sink = DummySQLSink(
            target,
            stream_name=stream_name,
            schema=schema,
            key_properties=["id"],
        )

        assert sink.table_name == expected_table_name
        assert sink.schema_name == expected_schema_name
