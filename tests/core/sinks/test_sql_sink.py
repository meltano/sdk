from __future__ import annotations

import typing as t
from textwrap import dedent

import pytest

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


@pytest.mark.xfail
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
        expected = dedent(
            """\
            INSERT INTO foo
            (id, col_ts, "table")
            VALUES (:id, :col_ts, :table)"""
        )
        assert sink.generate_insert_statement("foo", schema=schema) == expected
