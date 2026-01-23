"""Tests for ACTIVATE_VERSION handlers."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
import sqlalchemy as sa

from singer_sdk.sql.load_methods.context import LoadContext
from singer_sdk.sql.load_methods.handlers import (
    HardDeleteHandler,
    NullHandler,
    SoftDeleteHandler,
)


@pytest.fixture
def basic_context() -> LoadContext:
    """Create a basic LoadContext for testing."""
    return LoadContext(
        key_properties=("id",),
        soft_delete_column="_sdc_deleted_at",
        version_column="_sdc_table_version",
    )


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


class TestSoftDeleteHandler:
    """Tests for SoftDeleteHandler."""

    def test_activate_version_table_not_exists(
        self,
        mock_table: sa.Table,
        basic_context: LoadContext,
        mock_engine: MagicMock,
    ) -> None:
        """Test activate_version when table doesn't exist."""
        with patch(
            "singer_sdk.sql.load_methods.handlers._table_exists", return_value=False
        ):
            handler = SoftDeleteHandler()
            result = handler.activate_version(
                mock_table, basic_context, 123, mock_engine
            )

            assert result.version_activated == 123
            assert result.records_soft_deleted == 0

    def test_activate_version_creates_columns(
        self,
        mock_table: sa.Table,
        basic_context: LoadContext,
        mock_engine: MagicMock,
    ) -> None:
        """Test activate_version creates missing columns."""
        mock_result = MagicMock()
        mock_result.rowcount = 5

        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=None)
        mock_conn.execute.return_value = mock_result
        mock_engine.begin.return_value = mock_conn

        with (
            patch(
                "singer_sdk.sql.load_methods.handlers._table_exists", return_value=True
            ),
            patch(
                "singer_sdk.sql.load_methods.handlers._column_exists",
                return_value=False,
            ),
            patch("singer_sdk.sql.load_methods.handlers._add_column") as mock_add,
        ):
            handler = SoftDeleteHandler()
            handler.activate_version(mock_table, basic_context, 123, mock_engine)

            # Should prepare both version and soft_delete columns
            assert mock_add.call_count == 2

    def test_activate_version_updates_records(
        self,
        mock_table: sa.Table,
        basic_context: LoadContext,
        mock_engine: MagicMock,
    ) -> None:
        """Test activate_version updates old version records."""
        mock_result = MagicMock()
        mock_result.rowcount = 10

        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=None)
        mock_conn.execute.return_value = mock_result
        mock_engine.begin.return_value = mock_conn

        with (
            patch(
                "singer_sdk.sql.load_methods.handlers._table_exists", return_value=True
            ),
            patch(
                "singer_sdk.sql.load_methods.handlers._column_exists", return_value=True
            ),
        ):
            handler = SoftDeleteHandler()
            result = handler.activate_version(
                mock_table, basic_context, 123, mock_engine
            )

            assert result.records_soft_deleted == 10
            assert result.version_activated == 123
            mock_conn.execute.assert_called_once()


class TestHardDeleteHandler:
    """Tests for HardDeleteHandler."""

    def test_activate_version_table_not_exists(
        self,
        mock_table: sa.Table,
        basic_context: LoadContext,
        mock_engine: MagicMock,
    ) -> None:
        """Test activate_version when table doesn't exist."""
        with patch(
            "singer_sdk.sql.load_methods.handlers._table_exists", return_value=False
        ):
            handler = HardDeleteHandler()
            result = handler.activate_version(
                mock_table, basic_context, 123, mock_engine
            )

            assert result.version_activated == 123

    def test_activate_version_creates_version_column(
        self,
        mock_table: sa.Table,
        basic_context: LoadContext,
        mock_engine: MagicMock,
    ) -> None:
        """Test activate_version creates version column if missing."""
        mock_result = MagicMock()
        mock_result.rowcount = 5

        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=None)
        mock_conn.execute.return_value = mock_result
        mock_engine.begin.return_value = mock_conn

        with (
            patch(
                "singer_sdk.sql.load_methods.handlers._table_exists", return_value=True
            ),
            patch(
                "singer_sdk.sql.load_methods.handlers._column_exists",
                return_value=False,
            ),
            patch("singer_sdk.sql.load_methods.handlers._add_column") as mock_add,
        ):
            handler = HardDeleteHandler()
            handler.activate_version(mock_table, basic_context, 123, mock_engine)

            mock_add.assert_called_once()

    def test_activate_version_deletes_records(
        self,
        mock_table: sa.Table,
        basic_context: LoadContext,
        mock_engine: MagicMock,
    ) -> None:
        """Test activate_version deletes old version records."""
        mock_result = MagicMock()
        mock_result.rowcount = 15

        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=None)
        mock_conn.execute.return_value = mock_result
        mock_engine.begin.return_value = mock_conn

        with (
            patch(
                "singer_sdk.sql.load_methods.handlers._table_exists", return_value=True
            ),
            patch(
                "singer_sdk.sql.load_methods.handlers._column_exists", return_value=True
            ),
        ):
            handler = HardDeleteHandler()
            result = handler.activate_version(
                mock_table, basic_context, 123, mock_engine
            )

            assert result.records_hard_deleted == 15
            assert result.version_activated == 123


class TestNullHandler:
    """Tests for NullHandler."""

    def test_activate_version_noop(
        self,
        mock_table: sa.Table,
        basic_context: LoadContext,
        mock_engine: MagicMock,
    ) -> None:
        """Test activate_version does nothing."""
        handler = NullHandler()
        result = handler.activate_version(mock_table, basic_context, 456, mock_engine)

        assert result.version_activated == 456
        assert result.records_soft_deleted == 0
        assert result.records_hard_deleted == 0
