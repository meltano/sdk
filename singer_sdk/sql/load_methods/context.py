"""Context dataclass for load method operations."""

from __future__ import annotations

import dataclasses
import typing as t
from dataclasses import dataclass, field


@dataclass(frozen=True)
class LoadContext:
    """Immutable context for load method operations.

    This dataclass encapsulates configuration and state needed by load method
    strategies. Table structure information is passed separately via SQLAlchemy
    Table objects, keeping this context focused on configuration.

    The context enables pure-function testing by providing all required state
    in an immutable container.
    """

    # Key properties for upsert operations
    key_properties: tuple[str, ...] = ()
    """Primary key column names as an immutable tuple."""

    # Configuration options
    hard_delete: bool = False
    """Whether to perform hard deletes (DELETE) vs soft deletes (timestamp marker)."""

    add_record_metadata: bool = False
    """Whether metadata columns are added to records."""

    # Metadata column names
    soft_delete_column: str = "_sdc_deleted_at"
    """Column name for soft delete timestamp markers."""

    version_column: str = "_sdc_table_version"
    """Column name for ACTIVATE_VERSION tracking."""

    # State tracking
    is_first_batch: bool = True
    """Whether this is the first batch being processed for this stream."""

    # Internal tracking for strategy state
    _batch_count: int = field(default=0, repr=False)
    """Number of batches processed so far."""

    def with_updates(self, **kwargs: t.Any) -> LoadContext:
        """Create a new LoadContext with updated values.

        Since LoadContext is immutable (frozen=True), this method creates a new
        instance with the specified fields updated.

        Args:
            **kwargs: Field names and their new values.

        Returns:
            A new LoadContext instance with the updated values.

        Example:
            >>> ctx = LoadContext(key_properties=("id",))
            >>> updated_ctx = ctx.with_updates(is_first_batch=False, _batch_count=1)
        """
        return dataclasses.replace(self, **kwargs)

    @property
    def has_key_properties(self) -> bool:
        """Check if key properties are defined.

        Returns:
            True if at least one key property is defined.
        """
        return len(self.key_properties) > 0


@dataclass(frozen=True)
class TablePreparationResult:
    """Result of table preparation operations.

    Contains information about what actions were taken during table preparation.
    """

    table_created: bool = False
    """Whether the table was newly created."""

    table_dropped: bool = False
    """Whether the table was dropped (for OVERWRITE mode)."""

    columns_added: tuple[str, ...] = ()
    """Names of columns that were added to the table."""

    primary_key_set: bool = False
    """Whether the primary key constraint was set/updated."""


@dataclass(frozen=True)
class BatchResult:
    """Result of processing a batch of records.

    Contains metrics and information about the batch processing operation.
    """

    records_processed: int = 0
    """Number of records that were processed."""

    records_inserted: int = 0
    """Number of records inserted (for APPEND and UPSERT modes)."""

    records_updated: int = 0
    """Number of records updated (for UPSERT mode)."""

    records_deleted: int = 0
    """Number of records deleted (for hard delete operations)."""

    duplicates_merged: int = 0
    """Number of duplicate records that were merged."""


@dataclass(frozen=True)
class ActivateVersionResult:
    """Result of ACTIVATE_VERSION message processing.

    Contains information about records affected by version activation.
    """

    records_soft_deleted: int = 0
    """Number of records marked with soft delete timestamp."""

    records_hard_deleted: int = 0
    """Number of records permanently deleted."""

    version_activated: int = 0
    """The version number that was activated."""
