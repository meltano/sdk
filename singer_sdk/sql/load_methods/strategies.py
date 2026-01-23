"""Load method strategy implementations."""

from __future__ import annotations

import typing as t
from abc import ABC, abstractmethod

import sqlalchemy as sa

from singer_sdk.sql.load_methods.context import (
    BatchResult,
    TablePreparationResult,
)

if t.TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlalchemy.engine import Engine

    from singer_sdk.sql.load_methods.context import LoadContext
    from singer_sdk.sql.load_methods.dialect import DialectHelper


def _table_exists(engine: Engine, table: sa.Table) -> bool:
    """Check if a table exists using SQLAlchemy inspection.

    Args:
        engine: SQLAlchemy Engine.
        table: SQLAlchemy Table object.

    Returns:
        True if the table exists.
    """
    inspector = sa.inspect(engine)
    return inspector.has_table(table.name, schema=table.schema)


def _get_missing_columns(engine: Engine, table: sa.Table) -> list[sa.Column]:
    """Get columns that exist in the Table but not in the physical database.

    Args:
        engine: SQLAlchemy Engine.
        table: SQLAlchemy Table object with desired schema.

    Returns:
        List of Column objects that need to be added.
    """
    inspector = sa.inspect(engine)
    try:
        existing_columns = {
            col["name"]
            for col in inspector.get_columns(table.name, schema=table.schema)
        }
    except sa.exc.NoSuchTableError:
        return list(table.columns)

    return [col for col in table.columns if col.name not in existing_columns]


def _add_column(engine: Engine, table: sa.Table, column: sa.Column) -> None:
    """Add a column to an existing table.

    Args:
        engine: SQLAlchemy Engine.
        table: SQLAlchemy Table object.
        column: Column to add.
    """
    full_table = f"{table.schema}.{table.name}" if table.schema else table.name

    with engine.begin() as conn:
        type_str = column.type.compile(dialect=engine.dialect)
        conn.execute(
            sa.text(f"ALTER TABLE {full_table} ADD COLUMN {column.name} {type_str}")
        )


class BaseStrategy(ABC):
    """Abstract base class for load method strategies.

    Provides common functionality shared by all strategies, including
    table preparation helpers and bulk insert operations.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Return the strategy name."""
        ...

    @abstractmethod
    def supports_activate_version(self) -> bool:
        """Check if this strategy supports ACTIVATE_VERSION messages."""
        ...

    def prepare_table(  # noqa: PLR6301
        self,
        table: sa.Table,
        context: LoadContext,  # noqa: ARG002
        engine: Engine,
    ) -> TablePreparationResult:
        """Prepare the target table for data loading.

        Default implementation creates the table if it doesn't exist
        and adds any missing columns.

        Args:
            table: SQLAlchemy Table object representing desired schema.
            context: The load context (unused in base implementation).
            engine: SQLAlchemy Engine.

        Returns:
            TablePreparationResult describing actions taken.
        """
        table_created = False
        columns_added: list[str] = []

        if not _table_exists(engine, table):
            table.create(engine)
            table_created = True
        else:
            # Add missing columns
            for column in _get_missing_columns(engine, table):
                _add_column(engine, table, column)
                columns_added.append(column.name)

        return TablePreparationResult(
            table_created=table_created,
            columns_added=tuple(columns_added),
        )

    def _bulk_insert(  # noqa: PLR6301
        self,
        table: sa.Table,
        records: Sequence[dict[str, t.Any]],
        engine: Engine,
    ) -> int:
        """Perform a bulk INSERT operation.

        Args:
            table: SQLAlchemy Table object.
            records: Records to insert.
            engine: SQLAlchemy Engine.

        Returns:
            Number of rows affected.
        """
        if not records:
            return 0

        column_names = [col.name for col in table.columns]
        insert_stmt = sa.insert(table)

        # Normalize records to have all columns
        normalized_records = [
            {name: record.get(name) for name in column_names} for record in records
        ]

        with engine.begin() as conn:
            result = conn.execute(insert_stmt, normalized_records)

        return result.rowcount or 0

    def handle_soft_delete_record(  # noqa: PLR6301
        self,
        context: LoadContext,  # noqa: ARG002
        record: dict[str, t.Any],
    ) -> dict[str, t.Any] | None:
        """Handle a record with soft delete marker.

        Default implementation returns the record as-is for processing.

        Args:
            context: The load context.
            record: The record with deletion marker.

        Returns:
            The record to process, or None to skip.
        """
        return record


class AppendStrategy(BaseStrategy):
    """Append-only load method strategy.

    This strategy always INSERTs all records, creating duplicates if
    the same record is sent multiple times. This is the simplest and
    fastest strategy but does not support deduplication.

    Use cases:
    - Append-only event logs
    - Time-series data where duplicates are acceptable
    - High-throughput scenarios where upsert overhead is prohibitive
    """

    @property
    def name(self) -> str:
        """Return the strategy name."""
        return "append-only"

    def supports_activate_version(self) -> bool:  # noqa: PLR6301
        """Append strategy supports ACTIVATE_VERSION.

        Returns:
            True, as append mode can mark/delete old versions.
        """
        return True

    def process_batch(
        self,
        table: sa.Table,
        context: LoadContext,  # noqa: ARG002
        records: Sequence[dict[str, t.Any]],
        engine: Engine,
        dialect: DialectHelper,  # noqa: ARG002
    ) -> BatchResult:
        """Process a batch by inserting all records.

        Args:
            table: SQLAlchemy Table object.
            context: The load context (unused).
            records: Records to insert.
            engine: SQLAlchemy Engine.
            dialect: DialectHelper (unused).

        Returns:
            BatchResult with insert count.
        """
        rows_affected = self._bulk_insert(table, records, engine)
        return BatchResult(
            records_processed=len(records),
            records_inserted=rows_affected,
        )

    def handle_soft_delete_record(  # noqa: PLR6301
        self,
        context: LoadContext,
        record: dict[str, t.Any],
    ) -> dict[str, t.Any] | None:
        """Handle soft delete in append mode.

        In append mode with hard_delete=True, we skip records marked for
        deletion since we can't update existing records.

        Args:
            context: The load context.
            record: The record with deletion marker.

        Returns:
            The record if soft delete mode, None if hard delete mode.
        """
        if context.hard_delete:
            # Can't delete in append mode - skip the record
            return None
        # In soft delete mode, append the record with the deletion marker
        return record


class UpsertStrategy(BaseStrategy):
    """Upsert (merge) load method strategy.

    This strategy updates existing records and inserts new ones based
    on the key_properties. Requires key_properties to be defined.

    The implementation uses native upsert syntax when available (PostgreSQL,
    MySQL, SQLite) or falls back to a staging table pattern.

    Use cases:
    - Dimension tables that need to reflect current state
    - Master data that gets updated
    - Any scenario requiring deduplication
    """

    @property
    def name(self) -> str:
        """Return the strategy name."""
        return "upsert"

    def supports_activate_version(self) -> bool:  # noqa: PLR6301
        """Upsert strategy supports ACTIVATE_VERSION.

        Returns:
            True, as upsert mode can mark/delete old versions.
        """
        return True

    def prepare_table(
        self,
        table: sa.Table,
        context: LoadContext,
        engine: Engine,
    ) -> TablePreparationResult:
        """Prepare table ensuring primary key exists.

        Args:
            table: SQLAlchemy Table object.
            context: The load context.
            engine: SQLAlchemy Engine.

        Returns:
            TablePreparationResult describing actions taken.

        Raises:
            ValueError: If key_properties are not defined.
        """
        if not context.has_key_properties:
            msg = (
                "Upsert strategy requires key_properties to be defined. "
                f"Table: {table.name}"
            )
            raise ValueError(msg)

        return super().prepare_table(table, context, engine)

    def process_batch(
        self,
        table: sa.Table,
        context: LoadContext,
        records: Sequence[dict[str, t.Any]],
        engine: Engine,
        dialect: DialectHelper,
    ) -> BatchResult:
        """Process a batch using upsert logic.

        Uses native upsert when the dialect supports it, otherwise
        falls back to a staging table pattern.

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

        if dialect.supports_native_upsert():
            return self._native_upsert(table, context, records, engine, dialect)

        return self._staging_table_upsert(table, context, records, engine)

    def _native_upsert(  # noqa: PLR6301
        self,
        table: sa.Table,
        context: LoadContext,
        records: Sequence[dict[str, t.Any]],
        engine: Engine,
        dialect: DialectHelper,
    ) -> BatchResult:
        """Perform upsert using native dialect support.

        Args:
            table: SQLAlchemy Table object.
            context: The load context.
            records: Records to upsert.
            engine: SQLAlchemy Engine.
            dialect: DialectHelper.

        Returns:
            BatchResult with metrics.
        """
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

    def _staging_table_upsert(  # noqa: PLR6301
        self,
        table: sa.Table,
        context: LoadContext,
        records: Sequence[dict[str, t.Any]],
        engine: Engine,
    ) -> BatchResult:
        """Perform upsert using staging table pattern.

        1. Create temp staging table
        2. Bulk insert into staging
        3. DELETE from target WHERE keys match staging
        4. INSERT INTO target SELECT * FROM staging
        5. Drop staging table

        Args:
            table: SQLAlchemy Table object.
            context: The load context.
            records: Records to upsert.
            engine: SQLAlchemy Engine.

        Returns:
            BatchResult with metrics.
        """
        column_names = [col.name for col in table.columns]
        staging_table_name = f"_singer_staging_{table.name}"

        # Create staging table with same structure
        staging_metadata = sa.MetaData()
        staging_table = sa.Table(
            staging_table_name,
            staging_metadata,
            *[col.copy() for col in table.columns],
            schema=table.schema,
        )

        try:
            staging_table.create(engine)

            # Insert into staging
            normalized_records = [
                {name: record.get(name) for name in column_names} for record in records
            ]

            with engine.begin() as conn:
                conn.execute(sa.insert(staging_table), normalized_records)

            # Build table references
            target_ref = f"{table.schema}.{table.name}" if table.schema else table.name
            staging_ref = (
                f"{table.schema}.{staging_table_name}"
                if table.schema
                else staging_table_name
            )

            # DELETE matching keys
            key_conditions = " AND ".join(
                f"{target_ref}.{key} = {staging_ref}.{key}"
                for key in context.key_properties
            )
            delete_sql = sa.text(
                f"DELETE FROM {target_ref} "  # noqa: S608
                f"WHERE EXISTS (SELECT 1 FROM {staging_ref} "
                f"WHERE {key_conditions})"
            )

            # INSERT from staging
            columns_str = ", ".join(column_names)
            insert_sql = sa.text(
                f"INSERT INTO {target_ref} ({columns_str}) "  # noqa: S608
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

    def handle_soft_delete_record(  # noqa: PLR6301
        self,
        context: LoadContext,  # noqa: ARG002
        record: dict[str, t.Any],
    ) -> dict[str, t.Any] | None:
        """Handle soft delete in upsert mode.

        In upsert mode:
        - hard_delete=True: Delete the record from target
        - hard_delete=False: Upsert the record with deletion marker

        Args:
            context: The load context.
            record: The record with deletion marker.

        Returns:
            The record if soft delete, None if handled via hard delete.
        """
        # For soft delete, upsert will update the record with the marker
        # For hard delete, we need to delete the record - handled separately
        return record


class OverwriteStrategy(BaseStrategy):
    """Overwrite (truncate and reload) load method strategy.

    This strategy drops and recreates the table on the first batch,
    then inserts all subsequent batches. This ensures the target table
    contains only the data from the current sync.

    Use cases:
    - Full table replication where incremental isn't possible
    - Small lookup tables that change completely
    - Testing and development scenarios
    """

    @property
    def name(self) -> str:
        """Return the strategy name."""
        return "overwrite"

    def supports_activate_version(self) -> bool:  # noqa: PLR6301
        """Overwrite strategy does not need ACTIVATE_VERSION.

        Since the table is dropped and recreated, there are no
        old versions to clean up.

        Returns:
            False, as overwrite mode doesn't use version tracking.
        """
        return False

    def prepare_table(
        self,
        table: sa.Table,
        context: LoadContext,
        engine: Engine,
    ) -> TablePreparationResult:
        """Prepare table with drop/recreate on first batch.

        On the first batch:
        1. Drop the existing table (if any)
        2. Create a new empty table

        On subsequent batches:
        - Only add missing columns (for schema changes mid-sync)

        Args:
            table: SQLAlchemy Table object.
            context: The load context.
            engine: SQLAlchemy Engine.

        Returns:
            TablePreparationResult describing actions taken.
        """
        if context.is_first_batch:
            # Drop and recreate on first batch
            table.drop(engine, checkfirst=True)
            table.create(engine)

            return TablePreparationResult(
                table_dropped=True,
                table_created=True,
            )

        # For subsequent batches, just add missing columns
        return super().prepare_table(table, context, engine)

    def process_batch(
        self,
        table: sa.Table,
        context: LoadContext,  # noqa: ARG002
        records: Sequence[dict[str, t.Any]],
        engine: Engine,
        dialect: DialectHelper,  # noqa: ARG002
    ) -> BatchResult:
        """Process a batch by inserting all records.

        After the table is dropped/recreated in prepare_table,
        this simply inserts all records.

        Args:
            table: SQLAlchemy Table object.
            context: The load context (unused).
            records: Records to insert.
            engine: SQLAlchemy Engine.
            dialect: DialectHelper (unused).

        Returns:
            BatchResult with insert count.
        """
        rows_affected = self._bulk_insert(table, records, engine)
        return BatchResult(
            records_processed=len(records),
            records_inserted=rows_affected,
        )

    def handle_soft_delete_record(  # noqa: PLR6301
        self,
        context: LoadContext,  # noqa: ARG002
        record: dict[str, t.Any],  # noqa: ARG002
    ) -> dict[str, t.Any] | None:
        """Handle soft delete in overwrite mode.

        In overwrite mode, soft delete markers are ignored since
        the entire table is replaced. Records with deletion markers
        are simply not included.

        Args:
            context: The load context.
            record: The record with deletion marker.

        Returns:
            None, as deleted records are skipped in overwrite mode.
        """
        # In overwrite mode, skip records marked for deletion
        return None
