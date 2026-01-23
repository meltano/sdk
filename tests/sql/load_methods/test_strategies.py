"""Tests for load method strategies."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
import sqlalchemy as sa

from singer_sdk.sql.load_methods.context import LoadContext
from singer_sdk.sql.load_methods.dialect import GenericDialectHelper
from singer_sdk.sql.load_methods.strategies import (
    AppendStrategy,
    OverwriteStrategy,
    UpsertStrategy,
)


@pytest.fixture
def basic_context() -> LoadContext:
    """Create a basic LoadContext for testing."""
    return LoadContext(key_properties=("id",))


@pytest.fixture
def mock_table() -> sa.Table:
    """Create a mock SQLAlchemy Table."""
    metadata = sa.MetaData()
    return sa.Table(
        "test_table",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("name", sa.String),
        schema="test_schema",
    )


@pytest.fixture
def mock_engine() -> MagicMock:
    """Create a mock SQLAlchemy Engine."""
    engine = MagicMock(spec=sa.Engine)
    engine.dialect = MagicMock()
    engine.dialect.name = "sqlite"
    return engine


@pytest.fixture
def dialect_helper() -> GenericDialectHelper:
    """Create a GenericDialectHelper for testing."""
    return GenericDialectHelper()


class TestAppendStrategy:
    """Tests for AppendStrategy."""

    def test_name(self) -> None:
        """Test strategy name."""
        strategy = AppendStrategy()
        assert strategy.name == "append-only"

    def test_supports_activate_version(self) -> None:
        """Test that append supports ACTIVATE_VERSION."""
        strategy = AppendStrategy()
        assert strategy.supports_activate_version() is True

    def test_prepare_table_creates_new(
        self,
        mock_table: sa.Table,
        basic_context: LoadContext,
        mock_engine: MagicMock,
    ) -> None:
        """Test prepare_table creates table when it doesn't exist."""
        with patch(
            "singer_sdk.sql.load_methods.strategies._table_exists", return_value=False
        ):
            # Mock table.create
            mock_table.create = MagicMock()

            strategy = AppendStrategy()
            result = strategy.prepare_table(mock_table, basic_context, mock_engine)

            mock_table.create.assert_called_once_with(mock_engine)
            assert result.table_created is True

    def test_prepare_table_adds_columns(
        self,
        mock_table: sa.Table,
        basic_context: LoadContext,
        mock_engine: MagicMock,
    ) -> None:
        """Test prepare_table adds missing columns to existing table."""
        missing_col = sa.Column("new_col", sa.String)

        with (
            patch(
                "singer_sdk.sql.load_methods.strategies._table_exists",
                return_value=True,
            ),
            patch(
                "singer_sdk.sql.load_methods.strategies._get_missing_columns",
                return_value=[missing_col],
            ),
            patch("singer_sdk.sql.load_methods.strategies._add_column") as mock_add,
        ):
            strategy = AppendStrategy()
            result = strategy.prepare_table(mock_table, basic_context, mock_engine)

            mock_add.assert_called_once()
            assert result.table_created is False
            assert len(result.columns_added) == 1

    def test_process_batch_inserts(
        self,
        mock_table: sa.Table,
        basic_context: LoadContext,
        mock_engine: MagicMock,
        dialect_helper: GenericDialectHelper,
    ) -> None:
        """Test process_batch inserts all records."""
        mock_result = MagicMock()
        mock_result.rowcount = 3

        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=None)
        mock_conn.execute.return_value = mock_result
        mock_engine.begin.return_value = mock_conn

        records = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
            {"id": 3, "name": "Charlie"},
        ]

        strategy = AppendStrategy()
        result = strategy.process_batch(
            mock_table, basic_context, records, mock_engine, dialect_helper
        )

        assert result.records_processed == 3
        assert result.records_inserted == 3

    def test_handle_soft_delete_soft_mode(self, basic_context: LoadContext) -> None:
        """Test handle_soft_delete in soft delete mode returns record."""
        record = {"id": 1, "_sdc_deleted_at": "2024-01-01"}
        strategy = AppendStrategy()

        result = strategy.handle_soft_delete_record(basic_context, record)
        assert result == record

    def test_handle_soft_delete_hard_mode(self, basic_context: LoadContext) -> None:
        """Test handle_soft_delete in hard delete mode returns None."""
        context = basic_context.with_updates(hard_delete=True)
        record = {"id": 1, "_sdc_deleted_at": "2024-01-01"}

        strategy = AppendStrategy()
        result = strategy.handle_soft_delete_record(context, record)

        assert result is None


class TestUpsertStrategy:
    """Tests for UpsertStrategy."""

    def test_name(self) -> None:
        """Test strategy name."""
        strategy = UpsertStrategy()
        assert strategy.name == "upsert"

    def test_supports_activate_version(self) -> None:
        """Test that upsert supports ACTIVATE_VERSION."""
        strategy = UpsertStrategy()
        assert strategy.supports_activate_version() is True

    def test_prepare_table_requires_key_properties(
        self, mock_table: sa.Table, mock_engine: MagicMock
    ) -> None:
        """Test prepare_table raises when no key_properties."""
        context = LoadContext(key_properties=())  # Empty - no keys

        strategy = UpsertStrategy()

        with pytest.raises(ValueError, match="key_properties"):
            strategy.prepare_table(mock_table, context, mock_engine)

    def test_prepare_table_with_key_properties(
        self,
        mock_table: sa.Table,
        basic_context: LoadContext,
        mock_engine: MagicMock,
    ) -> None:
        """Test prepare_table works with key_properties defined."""
        with patch(
            "singer_sdk.sql.load_methods.strategies._table_exists", return_value=False
        ):
            mock_table.create = MagicMock()

            strategy = UpsertStrategy()
            result = strategy.prepare_table(mock_table, basic_context, mock_engine)

            mock_table.create.assert_called_once()
            assert result.table_created is True

    def test_handle_soft_delete(self, basic_context: LoadContext) -> None:
        """Test handle_soft_delete returns record for upsert."""
        record = {"id": 1, "_sdc_deleted_at": "2024-01-01"}

        strategy = UpsertStrategy()
        result = strategy.handle_soft_delete_record(basic_context, record)

        # Upsert should process the record to update with deletion marker
        assert result == record


class TestOverwriteStrategy:
    """Tests for OverwriteStrategy."""

    def test_name(self) -> None:
        """Test strategy name."""
        strategy = OverwriteStrategy()
        assert strategy.name == "overwrite"

    def test_does_not_support_activate_version(self) -> None:
        """Test that overwrite does not support ACTIVATE_VERSION."""
        strategy = OverwriteStrategy()
        assert strategy.supports_activate_version() is False

    def test_prepare_table_drops_on_first_batch(
        self,
        mock_table: sa.Table,
        basic_context: LoadContext,
        mock_engine: MagicMock,
    ) -> None:
        """Test prepare_table drops and recreates on first batch."""
        mock_table.drop = MagicMock()
        mock_table.create = MagicMock()

        strategy = OverwriteStrategy()
        result = strategy.prepare_table(mock_table, basic_context, mock_engine)

        mock_table.drop.assert_called_once()
        mock_table.create.assert_called_once()
        assert result.table_dropped is True
        assert result.table_created is True

    def test_prepare_table_adds_columns_subsequent_batch(
        self,
        mock_table: sa.Table,
        basic_context: LoadContext,
        mock_engine: MagicMock,
    ) -> None:
        """Test prepare_table only adds columns on subsequent batches."""
        context = basic_context.with_updates(is_first_batch=False)
        missing_col = sa.Column("new_col", sa.String)

        with (
            patch(
                "singer_sdk.sql.load_methods.strategies._table_exists",
                return_value=True,
            ),
            patch(
                "singer_sdk.sql.load_methods.strategies._get_missing_columns",
                return_value=[missing_col],
            ),
            patch("singer_sdk.sql.load_methods.strategies._add_column") as mock_add,
        ):
            strategy = OverwriteStrategy()
            result = strategy.prepare_table(mock_table, context, mock_engine)

        # Should not drop/create, just add columns
        assert result.table_dropped is False
        assert mock_add.call_count == 1

    def test_process_batch_inserts(
        self,
        mock_table: sa.Table,
        basic_context: LoadContext,
        mock_engine: MagicMock,
        dialect_helper: GenericDialectHelper,
    ) -> None:
        """Test process_batch inserts all records."""
        mock_result = MagicMock()
        mock_result.rowcount = 2

        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=None)
        mock_conn.execute.return_value = mock_result
        mock_engine.begin.return_value = mock_conn

        records = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]

        strategy = OverwriteStrategy()
        result = strategy.process_batch(
            mock_table, basic_context, records, mock_engine, dialect_helper
        )

        assert result.records_processed == 2
        assert result.records_inserted == 2

    def test_handle_soft_delete_skips(self, basic_context: LoadContext) -> None:
        """Test handle_soft_delete returns None (skips deleted records)."""
        record = {"id": 1, "_sdc_deleted_at": "2024-01-01"}

        strategy = OverwriteStrategy()
        result = strategy.handle_soft_delete_record(basic_context, record)

        # Overwrite mode skips deleted records
        assert result is None
