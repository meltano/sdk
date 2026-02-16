"""Tests for SQL load method strategies."""

from __future__ import annotations

import sys
import typing as t
from unittest.mock import patch

import pytest
import sqlalchemy as sa

from singer_sdk import SQLTarget
from singer_sdk.connectors.sql import SQLConnector
from singer_sdk.sinks.sql import SQLSink
from singer_sdk.sql.load_strategies import (
    AppendOnlyStrategy,
    OverwriteStrategy,
    UpsertStrategy,
)

if t.TYPE_CHECKING:
    from singer_sdk.sql.loaders import TempTableUpsertLoader

if sys.version_info >= (3, 12):
    from typing import override  # noqa: ICN003
else:
    from typing_extensions import override


class DummyConnector(SQLConnector):
    """Test connector with configurable capabilities."""

    allow_column_alter = True
    allow_overwrite = True
    allow_temp_tables = True
    allow_merge_upsert = True

    def create_empty_table(
        self,
        full_table_name: str,
        schema: dict,
        primary_keys: t.Sequence[str] | None = None,
        partition_keys: list[str] | None = None,
        as_temp_table: bool = False,  # noqa: FBT002
    ) -> None:
        """Create empty table with support for temp tables."""
        if as_temp_table:
            # SQLite temp tables
            _, schema_name, table_name = self.parse_full_table_name(full_table_name)
            meta = sa.MetaData(schema=schema_name)
            columns = []
            for property_name, property_jsonschema in schema["properties"].items():
                columns.append(
                    sa.Column(
                        property_name,
                        self.to_sql_type(property_jsonschema),
                    )
                )
            temp_table = sa.Table(table_name, meta, *columns, prefixes=["TEMPORARY"])
            temp_table.create(self._engine, checkfirst=True)
            return

        # Use parent implementation for regular tables
        super().create_empty_table(
            full_table_name=full_table_name,
            schema=schema,
            primary_keys=primary_keys,
            partition_keys=partition_keys,
            as_temp_table=as_temp_table,
        )


class DummySink(SQLSink):
    """Test sink."""

    connector_class = DummyConnector


class DummyTarget(SQLTarget):
    """Test target."""

    name = "test-target"
    config_jsonschema: t.ClassVar[dict] = {"type": "object", "properties": {}}
    default_sink_class = DummySink


# ============================================================================
# AppendOnlyStrategy Tests
# ============================================================================


class TestAppendOnlyStrategy:
    """Tests for AppendOnlyStrategy."""

    @pytest.fixture
    def target(self) -> DummyTarget:
        """Create a test target."""
        return DummyTarget(config={"sqlalchemy_url": "sqlite:///"})

    @pytest.fixture
    def schema(self) -> dict[str, t.Any]:
        """Create a test schema."""
        return {
            "properties": {
                "id": {"type": ["string", "null"]},
                "name": {"type": ["string", "null"]},
                "age": {"type": ["integer", "null"]},
            },
        }

    @pytest.fixture
    def sink(self, target: DummyTarget, schema: dict[str, t.Any]) -> DummySink:
        """Create a test sink."""
        return DummySink(
            target,
            stream_name="test_stream",
            schema=schema,
            key_properties=["id"],
        )

    @pytest.fixture
    def strategy(self, sink: DummySink) -> AppendOnlyStrategy:
        """Create an AppendOnlyStrategy instance."""
        return AppendOnlyStrategy(connector=sink.connector, sink=sink)

    def test_validate_config(self, strategy: AppendOnlyStrategy):
        """Test that validate_config always passes for append-only."""
        # Should not raise any exception
        strategy.validate_config()

    def test_prepare_table_creates_new_table(
        self,
        strategy: AppendOnlyStrategy,
        sink: DummySink,
        schema: dict[str, t.Any],
    ):
        """Test prepare_table creates a new table when it doesn't exist."""
        full_table_name = sink.full_table_name
        primary_keys = ["id"]

        # Verify table doesn't exist
        assert not sink.connector.table_exists(full_table_name)

        # Prepare table
        strategy.prepare_table(full_table_name, schema, primary_keys)

        # Verify table was created
        assert sink.connector.table_exists(full_table_name)
        table = sink.connector.get_table(full_table_name)
        assert "id" in table.columns
        assert "name" in table.columns
        assert "age" in table.columns

    def test_prepare_table_adds_columns_to_existing_table(
        self,
        strategy: AppendOnlyStrategy,
        sink: DummySink,
        schema: dict[str, t.Any],
    ):
        """Test prepare_table adds new columns to existing table."""
        full_table_name = sink.full_table_name
        primary_keys = ["id"]

        # Create table with initial schema
        initial_schema = {
            "properties": {
                "id": {"type": ["string", "null"]},
                "name": {"type": ["string", "null"]},
            },
        }
        sink.connector.create_empty_table(
            full_table_name=full_table_name,
            schema=initial_schema,
            primary_keys=primary_keys,
            as_temp_table=False,
        )

        # Verify initial columns
        table = sink.connector.get_table(full_table_name)
        assert "id" in table.columns
        assert "name" in table.columns
        assert "age" not in table.columns

        # Prepare table with new schema (adds age column)
        strategy.prepare_table(full_table_name, schema, primary_keys)

        # Verify age column was added
        table = sink.connector.get_table(full_table_name)
        assert "id" in table.columns
        assert "name" in table.columns
        assert "age" in table.columns

    def test_load_batch_inserts_records(
        self,
        strategy: AppendOnlyStrategy,
        sink: DummySink,
        schema: dict[str, t.Any],
    ):
        """Test load_batch inserts records correctly."""
        full_table_name = sink.full_table_name
        primary_keys = ["id"]

        # Prepare table
        strategy.prepare_table(full_table_name, schema, primary_keys)

        # Load batch
        records = [
            {"id": "1", "name": "Alice", "age": 30},
            {"id": "2", "name": "Bob", "age": 25},
        ]
        result = strategy.load_batch(full_table_name, schema, records)

        # Verify records were inserted
        assert result == 2

        # Verify data in table
        with sink.connector._connect() as conn:
            query = sa.text(f"SELECT * FROM {full_table_name} ORDER BY id")  # noqa: S608
            rows = conn.execute(query).fetchall()
            assert len(rows) == 2
            assert rows[0][0] == "1"
            assert rows[0][1] == "Alice"
            assert rows[1][0] == "2"
            assert rows[1][1] == "Bob"

    def test_load_batch_multiple_batches(
        self,
        strategy: AppendOnlyStrategy,
        sink: DummySink,
        schema: dict[str, t.Any],
    ):
        """Test that append-only can load multiple batches."""
        full_table_name = sink.full_table_name
        primary_keys = ["id"]

        # Prepare table
        strategy.prepare_table(full_table_name, schema, primary_keys)

        # Load first batch
        records1 = [{"id": "1", "name": "Alice", "age": 30}]
        strategy.load_batch(full_table_name, schema, records1)

        # Load second batch with different IDs
        records2 = [{"id": "2", "name": "Bob", "age": 25}]
        result = strategy.load_batch(full_table_name, schema, records2)

        # Verify both records are present
        assert result == 1
        with sink.connector._connect() as conn:
            query = sa.text(f"SELECT COUNT(*) FROM {full_table_name}")  # noqa: S608
            count = conn.execute(query).scalar()
            assert count == 2  # Both records should exist


# ============================================================================
# OverwriteStrategy Tests
# ============================================================================


class TestOverwriteStrategy:
    """Tests for OverwriteStrategy."""

    @pytest.fixture
    def target(self) -> DummyTarget:
        """Create a test target."""
        return DummyTarget(
            config={
                "sqlalchemy_url": "sqlite:///",
                "load_method": "overwrite",
            }
        )

    @pytest.fixture
    def schema(self) -> dict[str, t.Any]:
        """Create a test schema."""
        return {
            "properties": {
                "id": {"type": ["string", "null"]},
                "name": {"type": ["string", "null"]},
            },
        }

    @pytest.fixture
    def sink(self, target: DummyTarget, schema: dict[str, t.Any]) -> DummySink:
        """Create a test sink."""
        return DummySink(
            target,
            stream_name="test_stream",
            schema=schema,
            key_properties=["id"],
        )

    @pytest.fixture
    def strategy(self, sink: DummySink) -> OverwriteStrategy:
        """Create an OverwriteStrategy instance."""
        return OverwriteStrategy(connector=sink.connector, sink=sink)

    def test_validate_config_allows_overwrite(self, strategy: OverwriteStrategy):
        """Test validate_config passes when allow_overwrite is True."""
        # Should not raise (connector has allow_overwrite=True)
        strategy.validate_config()

    def test_validate_config_rejects_when_not_allowed(self, sink: DummySink):
        """Test validate_config fails when allow_overwrite is False."""

        class NoOverwriteConnector(DummyConnector):
            allow_overwrite = False

        connector = NoOverwriteConnector(config={"sqlalchemy_url": "sqlite:///"})
        strategy = OverwriteStrategy(connector=connector, sink=sink)

        with pytest.raises(ValueError, match="OVERWRITE load method is not supported"):
            strategy.validate_config()

    def test_prepare_table_drops_and_creates_first_time(
        self,
        strategy: OverwriteStrategy,
        sink: DummySink,
        schema: dict[str, t.Any],
    ):
        """Test prepare_table drops and recreates table on first call."""
        full_table_name = sink.full_table_name
        primary_keys = ["id"]

        # Create table with data
        sink.connector.create_empty_table(
            full_table_name=full_table_name,
            schema=schema,
            primary_keys=primary_keys,
            as_temp_table=False,
        )
        with sink.connector._connect() as conn:
            conn.execute(
                sa.text(
                    f"INSERT INTO {full_table_name} (id, name) VALUES ('1', 'Alice')"
                )
            )
            conn.commit()

        # Verify data exists
        with sink.connector._connect() as conn:
            count = conn.execute(
                sa.text(f"SELECT COUNT(*) FROM {full_table_name}")  # noqa: S608
            ).scalar()
            assert count == 1

        # Prepare table (should drop and recreate)
        strategy.prepare_table(full_table_name, schema, primary_keys)

        # Verify table was recreated (data should be gone)
        assert sink.connector.table_exists(full_table_name)
        with sink.connector._connect() as conn:
            count = conn.execute(
                sa.text(f"SELECT COUNT(*) FROM {full_table_name}")  # noqa: S608
            ).scalar()
            assert count == 0

    def test_prepare_table_adds_columns_on_subsequent_calls(
        self,
        strategy: OverwriteStrategy,
        sink: DummySink,
        schema: dict[str, t.Any],
    ):
        """Test prepare_table adds columns without dropping on subsequent calls."""
        full_table_name = sink.full_table_name
        primary_keys = ["id"]

        # First preparation (drop + create)
        strategy.prepare_table(full_table_name, schema, primary_keys)

        # Insert data
        with sink.connector._connect() as conn:
            conn.execute(
                sa.text(
                    f"INSERT INTO {full_table_name} (id, name) VALUES ('1', 'Alice')"
                )
            )
            conn.commit()

        # Second preparation with new column (should NOT drop)
        new_schema = {
            "properties": {
                "id": {"type": ["string", "null"]},
                "name": {"type": ["string", "null"]},
                "age": {"type": ["integer", "null"]},
            },
        }
        strategy.prepare_table(full_table_name, new_schema, primary_keys)

        # Verify table exists and data is preserved
        table = sink.connector.get_table(full_table_name)
        assert "age" in table.columns
        with sink.connector._connect() as conn:
            count = conn.execute(
                sa.text(f"SELECT COUNT(*) FROM {full_table_name}")  # noqa: S608
            ).scalar()
            assert count == 1  # Data should still exist

    def test_load_batch_inserts_records(
        self,
        strategy: OverwriteStrategy,
        sink: DummySink,
        schema: dict[str, t.Any],
    ):
        """Test load_batch inserts records."""
        full_table_name = sink.full_table_name
        primary_keys = ["id"]

        # Prepare table
        strategy.prepare_table(full_table_name, schema, primary_keys)

        # Load batch
        records = [
            {"id": "1", "name": "Alice"},
            {"id": "2", "name": "Bob"},
        ]
        result = strategy.load_batch(full_table_name, schema, records)

        # Verify
        assert result == 2
        with sink.connector._connect() as conn:
            count = conn.execute(
                sa.text(f"SELECT COUNT(*) FROM {full_table_name}")  # noqa: S608
            ).scalar()
            assert count == 2


# ============================================================================
# UpsertStrategy Tests
# ============================================================================


class TestUpsertStrategy:
    """Tests for UpsertStrategy."""

    @pytest.fixture
    def target(self) -> DummyTarget:
        """Create a test target."""
        return DummyTarget(
            config={
                "sqlalchemy_url": "sqlite:///",
                "load_method": "upsert",
            }
        )

    @pytest.fixture
    def schema(self) -> dict[str, t.Any]:
        """Create a test schema."""
        return {
            "properties": {
                "id": {"type": ["string", "null"]},
                "name": {"type": ["string", "null"]},
                "age": {"type": ["integer", "null"]},
            },
        }

    @pytest.fixture
    def sink(self, target: DummyTarget, schema: dict[str, t.Any]) -> DummySink:
        """Create a test sink."""
        return DummySink(
            target,
            stream_name="test_stream",
            schema=schema,
            key_properties=["id"],
        )

    @pytest.fixture
    def strategy(self, sink: DummySink) -> UpsertStrategy:
        """Create a UpsertStrategy instance."""
        return UpsertStrategy(connector=sink.connector, sink=sink)

    def test_validate_config_passes_with_temp_tables(
        self,
        strategy: UpsertStrategy,
    ):
        """Test validate_config passes when temp tables are supported."""
        # Should not raise (connector has allow_temp_tables=True)
        strategy.validate_config()

    def test_validate_config_fails_without_primary_keys(
        self,
        target: DummyTarget,
        schema: dict[str, t.Any],
    ):
        """Test validate_config fails when primary keys are not defined."""
        sink = DummySink(
            target,
            stream_name="test_stream",
            schema=schema,
            key_properties=[],  # No primary keys
        )
        strategy = UpsertStrategy(connector=sink.connector, sink=sink)

        with pytest.raises(
            ValueError, match="UPSERT load method requires primary keys"
        ):
            strategy.validate_config()

    def test_validate_config_fails_without_capabilities(
        self,
        sink: DummySink,
    ):
        """Test validate_config fails when required capabilities are missing."""

        class NoCapabilitiesConnector(DummyConnector):
            allow_temp_tables = False
            allow_merge_upsert = False

        connector = NoCapabilitiesConnector(config={"sqlalchemy_url": "sqlite:///"})
        strategy = UpsertStrategy(connector=connector, sink=sink)

        with pytest.raises(ValueError, match="requires temp table support"):
            strategy.validate_config()

    def test_prepare_table_creates_new_table(
        self,
        strategy: UpsertStrategy,
        sink: DummySink,
        schema: dict[str, t.Any],
    ):
        """Test prepare_table creates a new table when it doesn't exist."""
        full_table_name = sink.full_table_name
        primary_keys = ["id"]

        # Verify table doesn't exist
        assert not sink.connector.table_exists(full_table_name)

        # Prepare table
        strategy.prepare_table(full_table_name, schema, primary_keys)

        # Verify table was created
        assert sink.connector.table_exists(full_table_name)

    def test_prepare_table_adds_columns_to_existing_table(
        self,
        strategy: UpsertStrategy,
        sink: DummySink,
        schema: dict[str, t.Any],
    ):
        """Test prepare_table adds new columns to existing table."""
        full_table_name = sink.full_table_name
        primary_keys = ["id"]

        # Create table with initial schema
        initial_schema = {
            "properties": {
                "id": {"type": ["string", "null"]},
                "name": {"type": ["string", "null"]},
            },
        }
        strategy.prepare_table(full_table_name, initial_schema, primary_keys)

        # Prepare table with new schema (adds age column)
        strategy.prepare_table(full_table_name, schema, primary_keys)

        # Verify age column was added
        table = sink.connector.get_table(full_table_name)
        assert "age" in table.columns

    def test_load_batch_upserts_with_single_key(
        self,
        strategy: UpsertStrategy,
        sink: DummySink,
        schema: dict[str, t.Any],
    ):
        """Test load_batch upserts records with single primary key."""
        full_table_name = sink.full_table_name
        primary_keys = ["id"]

        # Prepare table
        strategy.prepare_table(full_table_name, schema, primary_keys)

        # Insert initial records
        records1 = [
            {"id": "1", "name": "Alice", "age": 30},
            {"id": "2", "name": "Bob", "age": 25},
        ]
        strategy.load_batch(full_table_name, schema, records1)

        # Verify initial load
        with sink.connector._connect() as conn:
            result = conn.execute(
                sa.text(f"SELECT * FROM {full_table_name} ORDER BY id")  # noqa: S608
            )
            rows = result.fetchall()
            assert len(rows) == 2
            assert rows[0][1] == "Alice"
            assert rows[0][2] == 30

        # Upsert: update id=1, insert id=3
        records2 = [
            {"id": "1", "name": "Alice Updated", "age": 31},
            {"id": "3", "name": "Charlie", "age": 35},
        ]
        strategy.load_batch(full_table_name, schema, records2)

        # Verify upsert results
        with sink.connector._connect() as conn:
            result = conn.execute(
                sa.text(f"SELECT * FROM {full_table_name} ORDER BY id")  # noqa: S608
            )
            rows = result.fetchall()
            assert len(rows) == 3
            # id=1 should be updated
            assert rows[0][0] == "1"
            assert rows[0][1] == "Alice Updated"
            assert rows[0][2] == 31
            # id=2 unchanged
            assert rows[1][0] == "2"
            assert rows[1][1] == "Bob"
            # id=3 inserted
            assert rows[2][0] == "3"
            assert rows[2][1] == "Charlie"

    def test_load_batch_upserts_with_multiple_keys(
        self,
        target: DummyTarget,
    ):
        """Test load_batch upserts records with multiple primary keys."""
        schema = {
            "properties": {
                "user_id": {"type": ["string", "null"]},
                "project_id": {"type": ["string", "null"]},
                "role": {"type": ["string", "null"]},
            },
        }

        sink = DummySink(
            target,
            stream_name="test_multi_key",
            schema=schema,
            key_properties=["user_id", "project_id"],
        )
        strategy = UpsertStrategy(connector=sink.connector, sink=sink)
        full_table_name = sink.full_table_name
        primary_keys = ["user_id", "project_id"]

        # Prepare table
        strategy.prepare_table(full_table_name, schema, primary_keys)

        # Insert initial records
        records1 = [
            {"user_id": "u1", "project_id": "p1", "role": "admin"},
            {"user_id": "u1", "project_id": "p2", "role": "viewer"},
        ]
        strategy.load_batch(full_table_name, schema, records1)

        # Upsert: update (u1,p1), insert (u2,p1)
        records2 = [
            {"user_id": "u1", "project_id": "p1", "role": "owner"},
            {"user_id": "u2", "project_id": "p1", "role": "editor"},
        ]
        strategy.load_batch(full_table_name, schema, records2)

        # Verify upsert results
        with sink.connector._connect() as conn:
            result = conn.execute(
                sa.text(
                    f"SELECT * FROM {full_table_name} ORDER BY user_id, project_id"  # noqa: S608
                )
            )
            rows = result.fetchall()
            assert len(rows) == 3
            # (u1,p1) should be updated to owner
            assert rows[0][2] == "owner"
            # (u1,p2) unchanged
            assert rows[1][2] == "viewer"
            # (u2,p1) inserted
            assert rows[2][0] == "u2"
            assert rows[2][2] == "editor"

    def test_load_batch_with_empty_records(
        self,
        strategy: UpsertStrategy,
        sink: DummySink,
        schema: dict[str, t.Any],
    ):
        """Test load_batch handles empty record list."""
        full_table_name = sink.full_table_name
        primary_keys = ["id"]

        # Prepare table
        strategy.prepare_table(full_table_name, schema, primary_keys)

        # Load empty batch
        result = strategy.load_batch(full_table_name, schema, [])

        # Should return 0
        assert result == 0

    def test_temp_table_cleanup_on_error(
        self,
        strategy: UpsertStrategy,
        sink: DummySink,
        schema: dict[str, t.Any],
    ):
        """Test that temp tables are cleaned up even when errors occur."""
        full_table_name = sink.full_table_name
        primary_keys = ["id"]

        # Prepare table
        strategy.prepare_table(full_table_name, schema, primary_keys)

        # Mock the loader's _merge_from_temp to raise an error
        with patch.object(
            strategy.loader,
            "_merge_from_temp",
            side_effect=Exception("Test error"),
        ):
            records = [{"id": "1", "name": "Alice", "age": 30}]

            with pytest.raises(Exception, match="Test error"):
                strategy.load_batch(full_table_name, schema, records)

        # Verify no temp tables remain
        with sink.connector._connect() as conn:
            result = conn.execute(
                sa.text(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%temp%'"
                )
            )
            temp_tables = result.fetchall()
            assert len(temp_tables) == 0

    def test_loader_build_delete_sql_single_key(self, strategy: UpsertStrategy):
        """Test loader _build_delete_sql with single primary key."""
        from singer_sdk.sql.loaders import TempTableUpsertLoader

        # Get the loader (should be TempTableUpsertLoader for generic strategy)
        assert isinstance(strategy.loader, TempTableUpsertLoader)
        loader = t.cast("TempTableUpsertLoader", strategy.loader)

        sql = loader._build_delete_sql("target", "temp", ["id"])
        assert "DELETE FROM target" in sql
        assert "WHERE id IN (SELECT id FROM temp)" in sql

    def test_loader_build_delete_sql_multiple_keys(self, strategy: UpsertStrategy):
        """Test loader _build_delete_sql with multiple primary keys."""

        loader = t.cast("TempTableUpsertLoader", strategy.loader)
        sql = loader._build_delete_sql("target", "temp", ["id", "name"])
        assert "DELETE FROM target" in sql
        assert "WHERE (id, name) IN (SELECT id, name FROM temp)" in sql

    def test_loader_build_insert_from_temp_sql(self, strategy: UpsertStrategy):
        """Test loader _build_insert_from_temp_sql."""

        loader = t.cast("TempTableUpsertLoader", strategy.loader)
        sql = loader._build_insert_from_temp_sql(
            "target", "temp", ["id", "name", "age"]
        )
        assert "INSERT INTO target (id, name, age)" in sql
        assert "SELECT id, name, age FROM temp" in sql

    def test_loader_create_temp_table_name(self, strategy: UpsertStrategy):
        """Test loader _create_temp_table_name generates unique names."""

        loader = t.cast("TempTableUpsertLoader", strategy.loader)
        name1 = loader._create_temp_table_name("test_table")
        name2 = loader._create_temp_table_name("test_table")

        assert "test_table_temp_" in name1
        assert "test_table_temp_" in name2
        assert name1 != name2  # Should be unique


# ============================================================================
# Strategy Factory Tests
# ============================================================================


class TestStrategyFactory:
    """Tests for load strategy factory methods."""

    @pytest.fixture
    def schema(self) -> dict[str, t.Any]:
        """Create a test schema."""
        return {
            "properties": {
                "id": {"type": ["string", "null"]},
            },
        }

    def test_create_append_only_strategy(self, schema: dict[str, t.Any]):
        """Test factory creates AppendOnlyStrategy."""
        target = DummyTarget(
            config={
                "sqlalchemy_url": "sqlite:///",
                "load_method": "append-only",
            }
        )
        sink = DummySink(
            target,
            stream_name="test",
            schema=schema,
            key_properties=["id"],
        )

        strategy = sink.connector._create_load_strategy(sink)
        assert isinstance(strategy, AppendOnlyStrategy)

    def test_create_overwrite_strategy(self, schema: dict[str, t.Any]):
        """Test factory creates OverwriteStrategy."""
        target = DummyTarget(
            config={
                "sqlalchemy_url": "sqlite:///",
                "load_method": "overwrite",
            }
        )
        sink = DummySink(
            target,
            stream_name="test",
            schema=schema,
            key_properties=["id"],
        )

        strategy = sink.connector._create_load_strategy(sink)
        assert isinstance(strategy, OverwriteStrategy)

    def test_create_upsert_strategy(self, schema: dict[str, t.Any]):
        """Test factory creates UpsertStrategy."""
        target = DummyTarget(
            config={
                "sqlalchemy_url": "sqlite:///",
                "load_method": "upsert",
            }
        )
        sink = DummySink(
            target,
            stream_name="test",
            schema=schema,
            key_properties=["id"],
        )

        strategy = sink.connector._create_load_strategy(sink)
        assert isinstance(strategy, UpsertStrategy)

    def test_create_strategy_with_unknown_method(self, schema: dict[str, t.Any]):
        """Test factory raises error for unknown load method."""
        target = DummyTarget(
            config={
                "sqlalchemy_url": "sqlite:///",
            },
            validate_config=False,  # Bypass config validation
        )
        sink = DummySink(
            target,
            stream_name="test",
            schema=schema,
            key_properties=["id"],
        )

        # Manually set an invalid load_method in the config
        sink.connector.config["load_method"] = "unknown-method"

        with pytest.raises(ValueError, match="Unknown load_method"):
            sink.connector._create_load_strategy(sink)

    def test_create_strategy_defaults_to_append_only(self, schema: dict[str, t.Any]):
        """Test factory defaults to AppendOnlyStrategy when no method specified."""
        target = DummyTarget(
            config={
                "sqlalchemy_url": "sqlite:///",
                # No load_method specified
            }
        )
        sink = DummySink(
            target,
            stream_name="test",
            schema=schema,
            key_properties=["id"],
        )

        strategy = sink.connector._create_load_strategy(sink)
        assert isinstance(strategy, AppendOnlyStrategy)


# ============================================================================
# Custom Merge Upsert Tests
# ============================================================================


class TestCustomMergeUpsert:
    """Tests for custom merge_upsert_from_table implementation."""

    @pytest.fixture
    def schema(self) -> dict[str, t.Any]:
        """Create a test schema."""
        return {
            "properties": {
                "id": {"type": ["string", "null"]},
                "name": {"type": ["string", "null"]},
            },
        }

    def test_upsert_uses_custom_merge_when_implemented(self, schema: dict[str, t.Any]):
        """Test that UpsertStrategy uses custom merge when available."""

        class CustomMergeSink(DummySink):
            """Sink with custom merge implementation."""

            @override
            def merge_upsert_from_table(
                self,
                target_table_name: str,
                from_table_name: str,
                join_keys: list[str],
            ) -> int | None:
                """Custom merge implementation."""
                # Simple implementation for testing
                with self.connector._connect() as conn:
                    # Delete existing
                    conn.execute(
                        sa.text(
                            f"DELETE FROM {target_table_name} WHERE id IN (SELECT id FROM {from_table_name})"  # noqa: S608
                        )
                    )
                    # Insert new
                    result = conn.execute(
                        sa.text(
                            f"INSERT INTO {target_table_name} SELECT * FROM {from_table_name}"  # noqa: S608
                        )
                    )
                    conn.commit()
                    return result.rowcount if result.rowcount >= 0 else None

        target = DummyTarget(
            config={
                "sqlalchemy_url": "sqlite:///",
                "load_method": "upsert",
            }
        )
        sink = CustomMergeSink(
            target,
            stream_name="test",
            schema=schema,
            key_properties=["id"],
        )

        strategy = UpsertStrategy(connector=sink.connector, sink=sink)
        full_table_name = sink.full_table_name
        primary_keys = ["id"]

        # Prepare table
        strategy.prepare_table(full_table_name, schema, primary_keys)

        # Insert initial record
        records1 = [{"id": "1", "name": "Alice"}]
        strategy.load_batch(full_table_name, schema, records1)

        # Upsert
        records2 = [{"id": "1", "name": "Alice Updated"}]
        result = strategy.load_batch(full_table_name, schema, records2)

        # Verify custom merge was used
        assert result == 1
        with sink.connector._connect() as conn:
            row = conn.execute(
                sa.text(f"SELECT name FROM {full_table_name} WHERE id = '1'")  # noqa: S608
            ).fetchone()
            assert row[0] == "Alice Updated"

    def test_validate_config_requires_allow_merge_upsert_for_custom_merge(
        self,
        schema: dict[str, t.Any],
    ):
        """Test that custom merge requires allow_merge_upsert capability."""

        class NoMergeConnector(DummyConnector):
            allow_merge_upsert = False

        class NoMergeSink(SQLSink):
            connector_class = NoMergeConnector

            @override
            def merge_upsert_from_table(
                self,
                target_table_name: str,
                from_table_name: str,
                join_keys: list[str],
            ) -> int | None:
                return None

        class NoMergeTarget(SQLTarget):
            name = "test-target-no-merge"
            config_jsonschema: t.ClassVar[dict] = {"type": "object", "properties": {}}
            default_sink_class = NoMergeSink

        target = NoMergeTarget(
            config={
                "sqlalchemy_url": "sqlite:///",
                "load_method": "upsert",
            }
        )
        sink = NoMergeSink(
            target,
            stream_name="test",
            schema=schema,
            key_properties=["id"],
        )

        strategy = UpsertStrategy(connector=sink.connector, sink=sink)

        with pytest.raises(ValueError, match="requires allow_merge_upsert = True"):
            strategy.validate_config()
