"""Protocol definitions for load method strategies."""

from __future__ import annotations

import typing as t

if t.TYPE_CHECKING:
    from collections.abc import Sequence

    import sqlalchemy as sa
    from sqlalchemy.engine import Engine

    from singer_sdk.sql.load_methods.context import (
        ActivateVersionResult,
        BatchResult,
        LoadContext,
        TablePreparationResult,
    )
    from singer_sdk.sql.load_methods.dialect import DialectHelper


@t.runtime_checkable
class LoadMethodStrategy(t.Protocol):
    """Protocol defining load method behavior for SQL targets.

    This protocol defines the interface that all load method strategies must implement.
    It supports three primary operations:

    1. Table preparation - Setting up the target table structure
    2. Batch processing - Loading records into the target table
    3. Soft delete handling - Processing records with deletion markers

    Implementations should be stateless where possible, with all required state
    passed through the LoadContext parameter.

    The interface uses SQLAlchemy Engine and a DialectHelper for database operations,
    making strategies portable across different database backends.

    Example:
        >>> class CustomStrategy:
        ...     @property
        ...     def name(self) -> str:
        ...         return "custom"
        ...
        ...     def prepare_table(self, table, context, engine):
        ...         # Custom table preparation logic
        ...         ...
        ...
        ...     def process_batch(self, table, context, records, engine, dialect):
        ...         # Custom batch processing logic
        ...         ...
    """

    @property
    def name(self) -> str:
        """Return the name of this load method strategy.

        Returns:
            A string identifier for this strategy (e.g., "append", "upsert").
        """
        ...

    def prepare_table(
        self,
        table: sa.Table,
        context: LoadContext,
        engine: Engine,
    ) -> TablePreparationResult:
        """Prepare the target table for data loading.

        This method is called before processing batches to ensure the target table
        exists and has the correct schema. The behavior varies by strategy:

        - APPEND: Create table if not exists, add missing columns
        - UPSERT: Same as APPEND, ensures primary key exists
        - OVERWRITE: Drop and recreate table on first batch

        Args:
            table: SQLAlchemy Table object representing the desired schema.
            context: The load context containing configuration and state.
            engine: SQLAlchemy Engine for database operations.

        Returns:
            A TablePreparationResult describing what actions were taken.
        """
        ...

    def process_batch(
        self,
        table: sa.Table,
        context: LoadContext,
        records: Sequence[dict[str, t.Any]],
        engine: Engine,
        dialect: DialectHelper,
    ) -> BatchResult:
        """Process a batch of records.

        This is the main method for loading data into the target table.
        The behavior varies by strategy:

        - APPEND: INSERT all records (may create duplicates)
        - UPSERT: INSERT or UPDATE based on key_properties
        - OVERWRITE: INSERT all records (table was cleared on first batch)

        Args:
            table: SQLAlchemy Table object for the target table.
            context: The load context containing configuration and state.
            records: The sequence of record dictionaries to process.
            engine: SQLAlchemy Engine for database operations.
            dialect: DialectHelper for dialect-specific operations.

        Returns:
            A BatchResult with metrics about the operation.
        """
        ...

    def handle_soft_delete_record(
        self,
        context: LoadContext,
        record: dict[str, t.Any],
    ) -> dict[str, t.Any] | None:
        """Handle a record that has a soft delete marker.

        This method is called for records that have `_sdc_deleted_at` set,
        indicating they should be treated as deletions.

        Args:
            context: The load context containing configuration.
            record: The record with a soft delete marker.

        Returns:
            The modified record to be processed, or None if the record
            should be skipped (e.g., for hard delete with APPEND mode).
        """
        ...

    def supports_activate_version(self) -> bool:
        """Check if this strategy supports ACTIVATE_VERSION messages.

        ACTIVATE_VERSION is used to mark old versions of records for
        deletion after a full sync completes.

        Returns:
            True if this strategy can handle ACTIVATE_VERSION messages.
        """
        ...


@t.runtime_checkable
class ActivateVersionHandler(t.Protocol):
    """Protocol for handling ACTIVATE_VERSION messages.

    This protocol is separate from LoadMethodStrategy to allow for
    composition - strategies can be paired with different handlers
    for soft vs. hard delete behavior.

    ACTIVATE_VERSION messages indicate that all records with a version
    older than the specified version should be marked as deleted (soft)
    or removed (hard).

    Example:
        >>> class CustomHandler:
        ...     def activate_version(self, table, context, new_version, engine):
        ...         # Mark old records as deleted
        ...         ...
    """

    def activate_version(
        self,
        table: sa.Table,
        context: LoadContext,
        new_version: int,
        engine: Engine,
    ) -> ActivateVersionResult:
        """Process an ACTIVATE_VERSION message.

        This method handles the logic for marking or deleting records
        that have a version older than the newly activated version.

        Args:
            table: SQLAlchemy Table object for the target table.
            context: The load context containing column names and configuration.
            new_version: The version number being activated.
            engine: SQLAlchemy Engine for database operations.

        Returns:
            An ActivateVersionResult describing what records were affected.
        """
        ...
