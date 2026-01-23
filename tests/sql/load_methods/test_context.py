"""Tests for LoadContext and result dataclasses."""

from __future__ import annotations

import pytest

from singer_sdk.sql.load_methods.context import (
    ActivateVersionResult,
    BatchResult,
    LoadContext,
    TablePreparationResult,
)


@pytest.fixture
def basic_context() -> LoadContext:
    """Create a basic LoadContext for testing."""
    return LoadContext(key_properties=("id",))


class TestLoadContext:
    """Tests for LoadContext dataclass."""

    def test_creation_with_defaults(self) -> None:
        """Test LoadContext creation with default values."""
        ctx = LoadContext()

        assert ctx.key_properties == ()
        assert ctx.hard_delete is False
        assert ctx.add_record_metadata is False
        assert ctx.soft_delete_column == "_sdc_deleted_at"
        assert ctx.version_column == "_sdc_table_version"
        assert ctx.is_first_batch is True
        assert ctx._batch_count == 0

    def test_creation_with_custom_values(self) -> None:
        """Test LoadContext creation with custom values."""
        ctx = LoadContext(
            key_properties=("id",),
            hard_delete=True,
            add_record_metadata=True,
            soft_delete_column="deleted_at",
            version_column="table_version",
            is_first_batch=False,
            _batch_count=5,
        )

        assert ctx.key_properties == ("id",)
        assert ctx.hard_delete is True
        assert ctx.add_record_metadata is True
        assert ctx.soft_delete_column == "deleted_at"
        assert ctx.version_column == "table_version"
        assert ctx.is_first_batch is False
        assert ctx._batch_count == 5

    def test_immutability(self, basic_context: LoadContext) -> None:
        """Test that LoadContext is immutable."""
        with pytest.raises(AttributeError):
            basic_context.hard_delete = True  # type: ignore[misc]

    def test_with_updates(self, basic_context: LoadContext) -> None:
        """Test creating a new context with updated values."""
        updated = basic_context.with_updates(
            is_first_batch=False,
            _batch_count=1,
        )

        # Original unchanged
        assert basic_context.is_first_batch is True
        assert basic_context._batch_count == 0

        # New context has updates
        assert updated.is_first_batch is False
        assert updated._batch_count == 1

        # Other fields preserved
        assert updated.key_properties == basic_context.key_properties

    def test_has_key_properties_true(self, basic_context: LoadContext) -> None:
        """Test has_key_properties returns True when keys defined."""
        assert basic_context.has_key_properties is True

    def test_has_key_properties_false(self) -> None:
        """Test has_key_properties returns False when no keys."""
        ctx = LoadContext()
        assert ctx.has_key_properties is False


class TestTablePreparationResult:
    """Tests for TablePreparationResult dataclass."""

    def test_defaults(self) -> None:
        """Test TablePreparationResult default values."""
        result = TablePreparationResult()

        assert result.table_created is False
        assert result.table_dropped is False
        assert result.columns_added == ()
        assert result.primary_key_set is False

    def test_custom_values(self) -> None:
        """Test TablePreparationResult with custom values."""
        result = TablePreparationResult(
            table_created=True,
            table_dropped=True,
            columns_added=("col1", "col2"),
            primary_key_set=True,
        )

        assert result.table_created is True
        assert result.table_dropped is True
        assert result.columns_added == ("col1", "col2")
        assert result.primary_key_set is True


class TestBatchResult:
    """Tests for BatchResult dataclass."""

    def test_defaults(self) -> None:
        """Test BatchResult default values."""
        result = BatchResult()

        assert result.records_processed == 0
        assert result.records_inserted == 0
        assert result.records_updated == 0
        assert result.records_deleted == 0
        assert result.duplicates_merged == 0

    def test_custom_values(self) -> None:
        """Test BatchResult with custom values."""
        result = BatchResult(
            records_processed=100,
            records_inserted=80,
            records_updated=20,
            records_deleted=5,
            duplicates_merged=10,
        )

        assert result.records_processed == 100
        assert result.records_inserted == 80
        assert result.records_updated == 20
        assert result.records_deleted == 5
        assert result.duplicates_merged == 10


class TestActivateVersionResult:
    """Tests for ActivateVersionResult dataclass."""

    def test_defaults(self) -> None:
        """Test ActivateVersionResult default values."""
        result = ActivateVersionResult()

        assert result.records_soft_deleted == 0
        assert result.records_hard_deleted == 0
        assert result.version_activated == 0

    def test_custom_values(self) -> None:
        """Test ActivateVersionResult with custom values."""
        result = ActivateVersionResult(
            records_soft_deleted=50,
            records_hard_deleted=0,
            version_activated=123,
        )

        assert result.records_soft_deleted == 50
        assert result.records_hard_deleted == 0
        assert result.version_activated == 123
