from __future__ import annotations

import sys
import typing as t

import pytest
import sqlalchemy
import sqlalchemy.engine
import sqlalchemy.types

from singer_sdk import SQLTarget
from singer_sdk.connectors.sql import SQLConnector
from singer_sdk.sinks.sql import SQLSink

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

    def test_schema_change_does_not_drop_table(self):
        """Test that schema changes don't cause table drops in OVERWRITE mode.

        This test verifies the complete fix for issue #3237 where a schema change
        mid-stream would cause the second sink to drop and recreate the table,
        losing data from the first sink that hadn't been drained yet.

        The test verifies that:
        1. First sink creates the table with initial schema
        2. Records are inserted by first sink
        3. Second sink (with new schema) does NOT drop the table
        4. Table retains records from first sink
        5. New columns are added to support the new schema
        """
        target = DummySQLTarget(
            config={
                "sqlalchemy_url": "sqlite:///",
                "load_method": "overwrite",
            }
        )

        initial_schema = {
            "properties": {
                "id": {"type": ["string", "null"]},
                "name": {"type": ["string", "null"]},
            },
        }

        # Create first sink with initial schema
        sink1: SQLSink = target.get_sink(
            "test_stream",
            schema=initial_schema,
            key_properties=["id"],
        )

        # Process a record through sink1 (this creates the table)
        record1 = {"id": "1", "name": "Alice"}
        _ = sink1._get_context(record1)

        # Verify table exists and has the expected columns
        assert sink1.connector.table_exists(sink1.full_table_name)
        table = sink1.connector.get_table(sink1.full_table_name)
        assert "id" in table.columns
        assert "name" in table.columns

        # Insert a record
        sink1.bulk_insert_records(
            full_table_name=sink1.full_table_name,
            schema=initial_schema,
            records=[record1],
        )

        # Now simulate a schema change - new schema has an additional column
        new_schema = {
            "properties": {
                "id": {"type": ["string", "null"]},
                "name": {"type": ["string", "null"]},
                "age": {"type": ["integer", "null"]},
            },
        }

        # Create second sink with new schema (this simulates what happens
        # when a schema message arrives mid-stream)
        sink2: SQLSink = target.get_sink(
            "test_stream",
            schema=new_schema,
            key_properties=["id"],
        )

        # Process a record through sink2
        record2 = {"id": "2", "name": "Bob", "age": 30}
        _ = sink2._get_context(record2)

        # CRITICAL: Table should still exist (not dropped and recreated)
        assert sink2.connector.table_exists(sink2.full_table_name)

        # Verify the table now has all three columns
        table: sqlalchemy.Table = sink2.connector.get_table(sink2.full_table_name)
        assert "id" in table.columns
        assert "name" in table.columns
        assert "age" in table.columns

        # CRITICAL: Verify that data from sink1 is still present
        # (this is the key test - data should NOT have been lost)
        with sink2.connector._connect() as conn:
            result: sqlalchemy.engine.CursorResult = conn.execute(
                sqlalchemy.text(f"SELECT * FROM {sink2.full_table_name}")  # noqa: S608
            )
            rows = result.fetchall()
            # Should have the record we inserted through sink1
            assert len(rows) == 1
            assert rows == [("1", "Alice", None)]

    def test_schema_change_with_undrained_records(self):
        """Test the exact scenario from PR #3340 comment.

        This test simulates the scenario described in
        https://github.com/meltano/sdk/pull/3340#issuecomment-3461649362:

        1. First sink drains some records (id=1,2)
        2. First sink has buffered but undrained record (id=3)
        3. Schema change occurs, new sink created
        4. New sink's table preparation should NOT drop the table
        5. First sink should still be able to drain its buffered record
        6. All records should be preserved
        """
        target = DummySQLTarget(
            config={
                "sqlalchemy_url": "sqlite:///",
                "load_method": "overwrite",
            }
        )

        initial_schema = {
            "properties": {
                "id": {"type": ["string", "null"]},
            },
        }

        # Create first sink with initial schema
        sink1: SQLSink = target.get_sink(
            "test_stream",
            schema=initial_schema,
            key_properties=["id"],
        )

        # Simulate processing and draining records 1-2
        _ = sink1._get_context({"id": "1"})
        sink1.bulk_insert_records(
            full_table_name=sink1.full_table_name,
            schema=initial_schema,
            records=[
                {"id": "1"},
                {"id": "2"},
            ],
        )

        # Verify records 1-2 are in the table
        with sink1.connector._connect() as conn:
            result = conn.execute(
                sqlalchemy.text(f"SELECT COUNT(*) as cnt FROM {sink1.full_table_name}")  # noqa: S608
            )
            count = next(iter(result)).cnt
            assert count == 2, "Should have 2 records after first drain"

        # Now simulate a schema change - new schema has an additional column
        new_schema = {
            "properties": {
                "id": {"type": ["string", "null"]},
                "name": {"type": ["string", "null"]},
            },
        }

        # Create second sink with new schema (simulates schema message mid-stream)
        sink2: SQLSink = target.get_sink(
            "test_stream",
            schema=new_schema,
            key_properties=["id"],
        )

        # Process records through sink2
        _ = sink2._get_context({"id": "4", "name": "Dave"})

        # CRITICAL: Records 1-2 should STILL be in the table (not lost to drop/recreate)
        with sink2.connector._connect() as conn:
            result = conn.execute(
                sqlalchemy.text(f"SELECT COUNT(*) as cnt FROM {sink2.full_table_name}")  # noqa: S608
            )
            count = next(iter(result)).cnt
            assert count == 2, "Records 1-2 should still exist after schema change"

        # Now simulate draining a buffered record from sink1 (with old schema)
        # This tests that the old sink can still drain to the evolved table
        sink1.bulk_insert_records(
            full_table_name=sink1.full_table_name,
            schema=initial_schema,
            records=[{"id": "3"}],
        )

        # Drain records from sink2
        sink2.bulk_insert_records(
            full_table_name=sink2.full_table_name,
            schema=new_schema,
            records=[{"id": "4", "name": "Dave"}, {"id": "5", "name": "Eve"}],
        )

        # Verify all records are present
        with sink2.connector._connect() as conn:
            result: sqlalchemy.engine.CursorResult = conn.execute(
                sqlalchemy.text(
                    f"SELECT id, name FROM {sink2.full_table_name} ORDER BY id"  # noqa: S608
                )
            )
            rows = result.fetchall()
            assert len(rows) == 5, "Should have all 5 records"

            # Records from both sinks should be present
            assert rows == [
                ("1", None),
                ("2", None),
                ("3", None),
                ("4", "Dave"),
                ("5", "Eve"),
            ]
