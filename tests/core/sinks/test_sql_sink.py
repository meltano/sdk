from __future__ import annotations

import typing as t

import pytest
from sqlalchemy.sql import Insert

from samples.sample_duckdb import DuckDBConnector
from singer_sdk.sinks.sql import SQLSink
from singer_sdk.target_base import SQLTarget


class DuckDBSink(SQLSink):
    connector_class = DuckDBConnector


class DuckDBTarget(SQLTarget):
    """DuckDB target class."""

    name = "sql-target-mock"
    config_jsonschema: t.ClassVar[dict] = {"type": "object", "properties": {}}
    default_sink_class = DuckDBSink


class TestDuckDBSink:
    @pytest.fixture
    def target(self) -> DuckDBTarget:
        return DuckDBTarget(config={"sqlalchemy_url": "duckdb:///"})

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
    def sink(self, target: DuckDBTarget, schema: dict) -> DuckDBSink:
        return DuckDBSink(
            target,
            stream_name="foo",
            schema=schema,
            key_properties=["id"],
        )

    def test_generate_insert_statement(self, sink: DuckDBSink, schema: dict):
        """Test that the insert statement is generated correctly."""
        stmt = sink.generate_insert_statement("foo", schema=schema)
        assert isinstance(stmt, Insert)
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
                "foo-bar",  # expected_table_name
                "test",  # expected_schema_name
                id="default-schema-2-part",
            ),
            pytest.param(
                "foo-bar-baz",  # stream_name
                "test",  # default_target_schema
                "foo-bar-baz",  # expected_table_name
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
        target = DuckDBTarget(
            config={
                "sqlalchemy_url": "duckdb:///",
                "default_target_schema": default_target_schema,
            },
        )

        sink = DuckDBSink(
            target,
            stream_name=stream_name,
            schema=schema,
            key_properties=["id"],
        )

        assert sink.table_name == expected_table_name
        assert sink.schema_name == expected_schema_name
