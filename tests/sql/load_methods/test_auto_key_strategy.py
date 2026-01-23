"""Tests for AutoKeyUpsertStrategy - a proof of concept for GitHub issue #3392.

This module demonstrates how the load_method architecture can support automatic
primary key generation for streams without explicit key properties.

See: https://github.com/meltano/sdk/issues/3392
"""

from __future__ import annotations

import hashlib
import json
import typing as t
from unittest.mock import MagicMock

import pytest
import sqlalchemy as sa

from singer_sdk.sql.load_methods.context import BatchResult, LoadContext
from singer_sdk.sql.load_methods.dialect import (
    GenericDialectHelper,
    SQLiteDialectHelper,
)
from singer_sdk.sql.load_methods.resolver import LoadMethodResolver
from singer_sdk.sql.load_methods.strategies import BaseStrategy

if t.TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlalchemy.engine import Engine

    from singer_sdk.sql.load_methods.dialect import DialectHelper


# ---------------------------------------------------------------------------
# AutoKeyUpsertStrategy Implementation
# ---------------------------------------------------------------------------


class AutoKeyUpsertStrategy(BaseStrategy):
    """Upsert strategy that auto-generates a primary key from record content.

    This strategy enables upsert operations for streams without explicit
    key properties by generating a deterministic hash-based `_sdc_id` column.

    The hash is computed from the record's non-metadata fields, allowing
    deduplication based on actual data content.

    Features:
    - SHA256 hash for collision resistance
    - Excludes `_sdc_*` metadata columns from hash computation
    - Sorts keys for deterministic hashing regardless of field order
    - Automatically adds `_sdc_id` to schema and records

    Example:
        >>> strategy = AutoKeyUpsertStrategy()
        >>> record = {"name": "Alice", "email": "alice@example.com"}
        >>> enriched = strategy._add_auto_key(record)
        >>> "_sdc_id" in enriched
        True
    """

    #: Name of the auto-generated key column
    AUTO_KEY_COLUMN = "_sdc_id"

    #: Prefix for metadata columns to exclude from hashing
    METADATA_PREFIX = "_sdc_"

    @property
    def name(self) -> str:
        """Return the strategy name."""
        return "auto-key-upsert"

    def supports_activate_version(self) -> bool:
        """Auto-key upsert supports ACTIVATE_VERSION.

        Returns:
            True, as this strategy can mark/delete old versions.
        """
        return True

    def _compute_record_hash(self, record: dict[str, t.Any]) -> str:
        """Compute a deterministic hash for a record.

        Generates a SHA256 hash from the record's non-metadata fields.
        Keys are sorted to ensure deterministic output regardless of
        dict ordering.

        Args:
            record: The record to hash.

        Returns:
            A hex-encoded SHA256 hash string.
        """
        # Filter out metadata columns
        data_fields = {
            k: v for k, v in record.items() if not k.startswith(self.METADATA_PREFIX)
        }

        # Sort keys for deterministic serialization
        sorted_data = json.dumps(data_fields, sort_keys=True, default=str)

        # Compute SHA256 hash
        return hashlib.sha256(sorted_data.encode("utf-8")).hexdigest()

    def _add_auto_key(self, record: dict[str, t.Any]) -> dict[str, t.Any]:
        """Add the auto-generated key to a record.

        Args:
            record: The original record.

        Returns:
            A new record dict with `_sdc_id` added.
        """
        record_hash = self._compute_record_hash(record)
        return {**record, self.AUTO_KEY_COLUMN: record_hash}

    def _enrich_table_with_auto_key(self, table: sa.Table) -> sa.Table:
        """Create a new table definition with the auto-key column added.

        Args:
            table: The original table definition.

        Returns:
            A new Table with `_sdc_id` as primary key.
        """
        # Check if auto key already exists
        if self.AUTO_KEY_COLUMN in [col.name for col in table.columns]:
            return table

        # Create new table with auto key as primary key
        # Note: We recreate columns instead of using deprecated copy()
        new_columns = [
            sa.Column(self.AUTO_KEY_COLUMN, sa.String(64), primary_key=True),
        ]

        # Add existing columns without primary_key flag
        for col in table.columns:
            new_columns.append(
                sa.Column(col.name, col.type, primary_key=False, nullable=col.nullable)
            )

        metadata = sa.MetaData()
        return sa.Table(
            table.name,
            metadata,
            *new_columns,
            schema=table.schema,
        )

    def _get_effective_context(self, context: LoadContext) -> LoadContext:
        """Create a context with the auto-key as the key property.

        Args:
            context: The original context.

        Returns:
            A new context with `_sdc_id` as the sole key property.
        """
        return context.with_updates(key_properties=(self.AUTO_KEY_COLUMN,))

    def prepare_table(
        self,
        table: sa.Table,
        context: LoadContext,
        engine: Engine,
    ) -> t.Any:
        """Prepare table with auto-generated key column.

        Args:
            table: SQLAlchemy Table object.
            context: The load context.
            engine: SQLAlchemy Engine.

        Returns:
            TablePreparationResult describing actions taken.
        """
        # Enrich table with auto-key column
        enriched_table = self._enrich_table_with_auto_key(table)
        effective_context = self._get_effective_context(context)

        # Use parent's prepare_table logic
        return super().prepare_table(enriched_table, effective_context, engine)

    def process_batch(
        self,
        table: sa.Table,
        context: LoadContext,
        records: Sequence[dict[str, t.Any]],
        engine: Engine,
        dialect: DialectHelper,
    ) -> BatchResult:
        """Process a batch with auto-generated keys.

        Enriches each record with an `_sdc_id` computed from its content,
        then performs upsert using that key.

        Args:
            table: SQLAlchemy Table object.
            context: The load context.
            records: Records to upsert.
            engine: SQLAlchemy Engine.
            dialect: DialectHelper.

        Returns:
            BatchResult with upsert metrics.
        """
        if not records:
            return BatchResult()

        # Add auto-key to each record
        enriched_records = [self._add_auto_key(record) for record in records]

        # Enrich table definition
        enriched_table = self._enrich_table_with_auto_key(table)
        effective_context = self._get_effective_context(context)

        # Perform upsert using native or staging table pattern
        if dialect.supports_native_upsert():
            return self._native_upsert(
                enriched_table,
                effective_context,
                enriched_records,
                engine,
                dialect,
            )

        return self._staging_table_upsert(
            enriched_table,
            effective_context,
            enriched_records,
            engine,
        )

    def _native_upsert(
        self,
        table: sa.Table,
        context: LoadContext,
        records: Sequence[dict[str, t.Any]],
        engine: Engine,
        dialect: DialectHelper,
    ) -> BatchResult:
        """Perform upsert using native dialect support."""
        column_names = [col.name for col in table.columns]
        upsert_stmt = dialect.generate_upsert_statement(
            table, list(context.key_properties)
        )

        normalized_records = [
            {name: record.get(name) for name in column_names} for record in records
        ]

        with engine.begin() as conn:
            result = conn.execute(upsert_stmt, normalized_records)

        return BatchResult(
            records_processed=len(records),
            records_inserted=result.rowcount or 0,
        )

    def _staging_table_upsert(
        self,
        table: sa.Table,
        context: LoadContext,
        records: Sequence[dict[str, t.Any]],
        engine: Engine,
    ) -> BatchResult:
        """Perform upsert using staging table pattern."""
        column_names = [col.name for col in table.columns]
        staging_table_name = f"_singer_staging_{table.name}"

        staging_metadata = sa.MetaData()
        staging_columns = [
            sa.Column(col.name, col.type, primary_key=col.primary_key)
            for col in table.columns
        ]
        staging_table = sa.Table(
            staging_table_name,
            staging_metadata,
            *staging_columns,
            schema=table.schema,
        )

        try:
            staging_table.create(engine)

            normalized_records = [
                {name: record.get(name) for name in column_names} for record in records
            ]

            with engine.begin() as conn:
                conn.execute(sa.insert(staging_table), normalized_records)

            target_ref = f"{table.schema}.{table.name}" if table.schema else table.name
            staging_ref = (
                f"{table.schema}.{staging_table_name}"
                if table.schema
                else staging_table_name
            )

            key_conditions = " AND ".join(
                f"{target_ref}.{key} = {staging_ref}.{key}"
                for key in context.key_properties
            )
            delete_sql = sa.text(
                f"DELETE FROM {target_ref} "
                f"WHERE EXISTS (SELECT 1 FROM {staging_ref} "
                f"WHERE {key_conditions})"
            )

            columns_str = ", ".join(column_names)
            insert_sql = sa.text(
                f"INSERT INTO {target_ref} ({columns_str}) "
                f"SELECT {columns_str} FROM {staging_ref}"
            )

            with engine.begin() as conn:
                delete_result = conn.execute(delete_sql)
                records_updated = delete_result.rowcount or 0
                insert_result = conn.execute(insert_sql)
                records_inserted = insert_result.rowcount or 0

        finally:
            staging_table.drop(engine, checkfirst=True)

        return BatchResult(
            records_processed=len(records),
            records_inserted=records_inserted - records_updated,
            records_updated=records_updated,
        )

    def handle_soft_delete_record(
        self,
        context: LoadContext,
        record: dict[str, t.Any],
    ) -> dict[str, t.Any] | None:
        """Handle soft delete by upserting with deletion marker.

        Args:
            context: The load context.
            record: The record with deletion marker.

        Returns:
            The record with auto-key for upserting.
        """
        # Always process - the auto-key allows us to update the record
        return record


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestAutoKeyGeneration:
    """Tests for the auto-key generation logic."""

    def test_compute_hash_deterministic(self) -> None:
        """Test that hash computation is deterministic."""
        strategy = AutoKeyUpsertStrategy()

        record = {"name": "Alice", "email": "alice@example.com", "age": 30}

        hash1 = strategy._compute_record_hash(record)
        hash2 = strategy._compute_record_hash(record)

        assert hash1 == hash2
        assert len(hash1) == 64  # SHA256 hex length

    def test_compute_hash_ignores_field_order(self) -> None:
        """Test that hash is same regardless of field order."""
        strategy = AutoKeyUpsertStrategy()

        record1 = {"name": "Alice", "email": "alice@example.com"}
        record2 = {"email": "alice@example.com", "name": "Alice"}

        assert strategy._compute_record_hash(record1) == strategy._compute_record_hash(
            record2
        )

    def test_compute_hash_excludes_sdc_columns(self) -> None:
        """Test that _sdc_* columns are excluded from hash."""
        strategy = AutoKeyUpsertStrategy()

        record_without_sdc = {"name": "Alice", "email": "alice@example.com"}
        record_with_sdc = {
            "name": "Alice",
            "email": "alice@example.com",
            "_sdc_received_at": "2024-01-01T00:00:00Z",
            "_sdc_table_version": 1,
        }

        assert strategy._compute_record_hash(
            record_without_sdc
        ) == strategy._compute_record_hash(record_with_sdc)

    def test_compute_hash_different_for_different_data(self) -> None:
        """Test that different data produces different hashes."""
        strategy = AutoKeyUpsertStrategy()

        record1 = {"name": "Alice"}
        record2 = {"name": "Bob"}

        assert strategy._compute_record_hash(record1) != strategy._compute_record_hash(
            record2
        )

    def test_add_auto_key(self) -> None:
        """Test that _sdc_id is added to record."""
        strategy = AutoKeyUpsertStrategy()

        record = {"name": "Alice", "email": "alice@example.com"}
        enriched = strategy._add_auto_key(record)

        assert "_sdc_id" in enriched
        assert enriched["name"] == "Alice"
        assert enriched["email"] == "alice@example.com"
        assert len(enriched["_sdc_id"]) == 64

    def test_add_auto_key_preserves_original(self) -> None:
        """Test that original record is not modified."""
        strategy = AutoKeyUpsertStrategy()

        record = {"name": "Alice"}
        strategy._add_auto_key(record)

        assert "_sdc_id" not in record


class TestAutoKeyTableEnrichment:
    """Tests for table schema enrichment."""

    def test_enrich_table_adds_auto_key_column(self) -> None:
        """Test that _sdc_id column is added to table."""
        strategy = AutoKeyUpsertStrategy()

        metadata = sa.MetaData()
        table = sa.Table(
            "test_table",
            metadata,
            sa.Column("name", sa.String),
            sa.Column("email", sa.String),
        )

        enriched = strategy._enrich_table_with_auto_key(table)

        column_names = [col.name for col in enriched.columns]
        assert "_sdc_id" in column_names
        assert column_names[0] == "_sdc_id"  # First column

    def test_enrich_table_sets_auto_key_as_primary(self) -> None:
        """Test that _sdc_id becomes the primary key."""
        strategy = AutoKeyUpsertStrategy()

        metadata = sa.MetaData()
        table = sa.Table(
            "test_table",
            metadata,
            sa.Column("id", sa.Integer, primary_key=True),
            sa.Column("name", sa.String),
        )

        enriched = strategy._enrich_table_with_auto_key(table)

        # Find the _sdc_id column
        sdc_id_col = next(col for col in enriched.columns if col.name == "_sdc_id")
        id_col = next(col for col in enriched.columns if col.name == "id")

        assert sdc_id_col.primary_key is True
        assert id_col.primary_key is False

    def test_enrich_table_idempotent(self) -> None:
        """Test that enriching twice doesn't duplicate column."""
        strategy = AutoKeyUpsertStrategy()

        metadata = sa.MetaData()
        table = sa.Table(
            "test_table",
            metadata,
            sa.Column("_sdc_id", sa.String(64), primary_key=True),
            sa.Column("name", sa.String),
        )

        enriched = strategy._enrich_table_with_auto_key(table)

        sdc_id_count = sum(1 for col in enriched.columns if col.name == "_sdc_id")
        assert sdc_id_count == 1


class TestAutoKeyContext:
    """Tests for context manipulation."""

    def test_effective_context_sets_key_property(self) -> None:
        """Test that effective context has _sdc_id as key."""
        strategy = AutoKeyUpsertStrategy()

        # Context with no key properties (the target use case)
        context = LoadContext(key_properties=())

        effective = strategy._get_effective_context(context)

        assert effective.key_properties == ("_sdc_id",)
        assert effective.has_key_properties is True

    def test_effective_context_overrides_existing_keys(self) -> None:
        """Test that existing key properties are replaced."""
        strategy = AutoKeyUpsertStrategy()

        context = LoadContext(key_properties=("id", "tenant_id"))

        effective = strategy._get_effective_context(context)

        assert effective.key_properties == ("_sdc_id",)


class TestAutoKeyStrategyBehavior:
    """Tests for overall strategy behavior."""

    def test_strategy_name(self) -> None:
        """Test strategy name."""
        strategy = AutoKeyUpsertStrategy()
        assert strategy.name == "auto-key-upsert"

    def test_supports_activate_version(self) -> None:
        """Test that strategy supports ACTIVATE_VERSION."""
        strategy = AutoKeyUpsertStrategy()
        assert strategy.supports_activate_version() is True

    def test_handle_soft_delete_returns_record(self) -> None:
        """Test that soft delete records are processed (not skipped)."""
        strategy = AutoKeyUpsertStrategy()
        context = LoadContext()

        record = {"name": "Alice", "_sdc_deleted_at": "2024-01-01"}
        result = strategy.handle_soft_delete_record(context, record)

        assert result == record


class TestAutoKeyProcessBatch:
    """Tests for batch processing."""

    @pytest.fixture
    def mock_table(self) -> sa.Table:
        """Create a test table without primary key."""
        metadata = sa.MetaData()
        return sa.Table(
            "test_table",
            metadata,
            sa.Column("name", sa.String),
            sa.Column("email", sa.String),
            schema="test_schema",
        )

    @pytest.fixture
    def mock_engine(self) -> MagicMock:
        """Create a mock SQLAlchemy Engine."""
        engine = MagicMock(spec=sa.Engine)
        engine.dialect = MagicMock()
        engine.dialect.name = "sqlite"
        return engine

    @pytest.fixture
    def context_no_keys(self) -> LoadContext:
        """Create a context with no key properties."""
        return LoadContext(key_properties=())

    def test_process_batch_enriches_records(
        self,
        mock_table: sa.Table,
        mock_engine: MagicMock,
        context_no_keys: LoadContext,
    ) -> None:
        """Test that records get _sdc_id added during processing."""
        dialect = SQLiteDialectHelper()

        mock_result = MagicMock()
        mock_result.rowcount = 2

        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=None)
        mock_conn.execute.return_value = mock_result
        mock_engine.begin.return_value = mock_conn

        records = [
            {"name": "Alice", "email": "alice@example.com"},
            {"name": "Bob", "email": "bob@example.com"},
        ]

        strategy = AutoKeyUpsertStrategy()

        # Capture the records passed to execute
        captured_records = []

        def capture_execute(stmt, params=None):
            if params:
                captured_records.extend(params)
            return mock_result

        mock_conn.execute.side_effect = capture_execute

        strategy.process_batch(
            mock_table, context_no_keys, records, mock_engine, dialect
        )

        # Verify records were enriched with _sdc_id
        assert len(captured_records) == 2
        assert all("_sdc_id" in r for r in captured_records)
        assert all(len(r["_sdc_id"]) == 64 for r in captured_records)

    def test_process_batch_empty_records(
        self,
        mock_table: sa.Table,
        mock_engine: MagicMock,
        context_no_keys: LoadContext,
    ) -> None:
        """Test that empty records list returns empty result."""
        dialect = GenericDialectHelper()
        strategy = AutoKeyUpsertStrategy()

        result = strategy.process_batch(
            mock_table, context_no_keys, [], mock_engine, dialect
        )

        assert result.records_processed == 0
        assert result.records_inserted == 0


class TestAutoKeyResolverIntegration:
    """Tests for integration with LoadMethodResolver."""

    def test_register_auto_key_strategy(self) -> None:
        """Test that AutoKeyUpsertStrategy can be registered."""
        # Register the strategy
        LoadMethodResolver.register_strategy("auto-key-upsert", AutoKeyUpsertStrategy)

        try:
            # Verify it's in the registry
            assert "auto-key-upsert" in LoadMethodResolver.list_strategies()

            # Resolve it from config
            config = {"load_method": "auto-key-upsert"}
            composite = LoadMethodResolver.resolve(config)

            assert composite.strategy.name == "auto-key-upsert"
            assert isinstance(composite.strategy, AutoKeyUpsertStrategy)

        finally:
            # Clean up - unregister the strategy
            LoadMethodResolver.unregister_strategy("auto-key-upsert")

    def test_resolver_with_hard_delete(self) -> None:
        """Test resolver pairs strategy with correct handler."""
        from singer_sdk.sql.load_methods.handlers import HardDeleteHandler

        LoadMethodResolver.register_strategy("auto-key-upsert", AutoKeyUpsertStrategy)

        try:
            config = {"load_method": "auto-key-upsert", "hard_delete": True}
            composite = LoadMethodResolver.resolve(config)

            assert isinstance(composite.activate_version_handler, HardDeleteHandler)

        finally:
            LoadMethodResolver.unregister_strategy("auto-key-upsert")


class TestAutoKeyDeduplication:
    """Tests demonstrating deduplication behavior."""

    def test_same_record_same_hash(self) -> None:
        """Test that identical records produce same hash (enables dedup)."""
        strategy = AutoKeyUpsertStrategy()

        record1 = {"name": "Alice", "email": "alice@example.com"}
        record2 = {"name": "Alice", "email": "alice@example.com"}

        enriched1 = strategy._add_auto_key(record1)
        enriched2 = strategy._add_auto_key(record2)

        # Same data = same _sdc_id = upsert will update, not duplicate
        assert enriched1["_sdc_id"] == enriched2["_sdc_id"]

    def test_updated_record_different_hash(self) -> None:
        """Test that updated records get new hash (new version stored)."""
        strategy = AutoKeyUpsertStrategy()

        record_v1 = {"name": "Alice", "email": "alice@example.com"}
        record_v2 = {"name": "Alice", "email": "alice.new@example.com"}

        enriched1 = strategy._add_auto_key(record_v1)
        enriched2 = strategy._add_auto_key(record_v2)

        # Different data = different _sdc_id = both versions stored
        # This is intentional - it preserves history of changes
        assert enriched1["_sdc_id"] != enriched2["_sdc_id"]

    def test_metadata_changes_dont_affect_hash(self) -> None:
        """Test that metadata changes don't create new records."""
        strategy = AutoKeyUpsertStrategy()

        record_sync1 = {
            "name": "Alice",
            "email": "alice@example.com",
            "_sdc_received_at": "2024-01-01T00:00:00Z",
        }
        record_sync2 = {
            "name": "Alice",
            "email": "alice@example.com",
            "_sdc_received_at": "2024-01-02T00:00:00Z",  # Different sync time
        }

        enriched1 = strategy._add_auto_key(record_sync1)
        enriched2 = strategy._add_auto_key(record_sync2)

        # Same business data = same _sdc_id, even with different metadata
        assert enriched1["_sdc_id"] == enriched2["_sdc_id"]
