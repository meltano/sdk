"""ACTIVATE_VERSION handler implementations."""

from __future__ import annotations

import typing as t

import sqlalchemy as sa

from singer_sdk.helpers._util import utc_now
from singer_sdk.sql.load_methods.context import ActivateVersionResult

if t.TYPE_CHECKING:
    from sqlalchemy.engine import Engine

    from singer_sdk.sql.load_methods.context import LoadContext


def _column_exists(engine: Engine, table: sa.Table, column_name: str) -> bool:
    """Check if a column exists in a table.

    Args:
        engine: SQLAlchemy Engine.
        table: SQLAlchemy Table object.
        column_name: Name of the column.

    Returns:
        True if the column exists.
    """
    inspector = sa.inspect(engine)
    try:
        columns = inspector.get_columns(table.name, schema=table.schema)
        return any(col["name"] == column_name for col in columns)
    except sa.exc.NoSuchTableError:
        return False


def _table_exists(engine: Engine, table: sa.Table) -> bool:
    """Check if a table exists.

    Args:
        engine: SQLAlchemy Engine.
        table: SQLAlchemy Table object.

    Returns:
        True if the table exists.
    """
    inspector = sa.inspect(engine)
    return inspector.has_table(table.name, schema=table.schema)


def _add_column(
    engine: Engine,
    table: sa.Table,
    column_name: str,
    sql_type: sa.types.TypeEngine,
) -> None:
    """Add a column to an existing table.

    Args:
        engine: SQLAlchemy Engine.
        table: SQLAlchemy Table object.
        column_name: Name of the column to add.
        sql_type: SQLAlchemy type for the column.
    """
    full_table = f"{table.schema}.{table.name}" if table.schema else table.name

    with engine.begin() as conn:
        type_str = sql_type.compile(dialect=engine.dialect)
        conn.execute(
            sa.text(f"ALTER TABLE {full_table} ADD COLUMN {column_name} {type_str}")
        )


class SoftDeleteHandler:
    """Handler that marks old version records with a deletion timestamp.

    This handler sets the `_sdc_deleted_at` column on records that have
    a version older than the newly activated version. Records are not
    physically deleted, allowing for historical analysis.

    Use cases:
    - Audit trails requiring visibility into deleted records
    - Data recovery scenarios
    - Historical reporting that needs to show deletions
    """

    def activate_version(  # noqa: PLR6301
        self,
        table: sa.Table,
        context: LoadContext,
        new_version: int,
        engine: Engine,
    ) -> ActivateVersionResult:
        """Mark old version records with deletion timestamp.

        Updates all records where:
        - version_column < new_version
        - soft_delete_column IS NULL (not already deleted)

        Args:
            table: SQLAlchemy Table object.
            context: The load context with column info.
            new_version: The version being activated.
            engine: SQLAlchemy Engine.

        Returns:
            ActivateVersionResult with count of soft-deleted records.
        """
        # Ensure the table exists
        if not _table_exists(engine, table):
            return ActivateVersionResult(version_activated=new_version)

        deleted_at = utc_now()

        # Ensure version column exists
        if not _column_exists(engine, table, context.version_column):
            _add_column(
                engine=engine,
                table=table,
                column_name=context.version_column,
                sql_type=sa.Integer(),
            )

        # Ensure soft delete column exists
        if not _column_exists(engine, table, context.soft_delete_column):
            _add_column(
                engine=engine,
                table=table,
                column_name=context.soft_delete_column,
                sql_type=sa.DateTime(),
            )

        # Build table reference
        table_ref = f"{table.schema}.{table.name}" if table.schema else table.name

        # Update records to mark as soft deleted
        query = sa.text(
            f"UPDATE {table_ref}\n"
            f"SET {context.soft_delete_column} = :deletedate \n"
            f"WHERE {context.version_column} < :version \n"
            f"  AND {context.soft_delete_column} IS NULL\n",
        )
        query = query.bindparams(
            sa.bindparam("deletedate", value=deleted_at, type_=sa.DateTime),
            sa.bindparam("version", value=new_version, type_=sa.Integer),
        )

        with engine.begin() as conn:
            result = conn.execute(query)
            rows_affected = result.rowcount or 0

        return ActivateVersionResult(
            records_soft_deleted=rows_affected,
            version_activated=new_version,
        )


class HardDeleteHandler:
    """Handler that physically deletes old version records.

    This handler DELETEs records that have a version older than the
    newly activated version. Records are permanently removed from
    the database.

    Use cases:
    - GDPR/privacy compliance requiring actual deletion
    - Storage optimization
    - Scenarios where historical deleted data is not needed
    """

    def activate_version(  # noqa: PLR6301
        self,
        table: sa.Table,
        context: LoadContext,
        new_version: int,
        engine: Engine,
    ) -> ActivateVersionResult:
        """Delete old version records.

        Deletes all records where version_column < new_version.

        Args:
            table: SQLAlchemy Table object.
            context: The load context with column info.
            new_version: The version being activated.
            engine: SQLAlchemy Engine.

        Returns:
            ActivateVersionResult with count of hard-deleted records.
        """
        # Ensure the table exists
        if not _table_exists(engine, table):
            return ActivateVersionResult(version_activated=new_version)

        # Ensure version column exists
        if not _column_exists(engine, table, context.version_column):
            _add_column(
                engine=engine,
                table=table,
                column_name=context.version_column,
                sql_type=sa.Integer(),
            )

        # Build table reference
        table_ref = f"{table.schema}.{table.name}" if table.schema else table.name

        # Delete old version records
        query = sa.text(
            f"DELETE FROM {table_ref} WHERE {context.version_column} < :version"  # noqa: S608
        )
        query = query.bindparams(
            sa.bindparam("version", value=new_version, type_=sa.Integer),
        )

        with engine.begin() as conn:
            result = conn.execute(query)
            rows_affected = result.rowcount or 0

        return ActivateVersionResult(
            records_hard_deleted=rows_affected,
            version_activated=new_version,
        )


class NullHandler:
    """Handler that does nothing with ACTIVATE_VERSION messages.

    This handler is used when ACTIVATE_VERSION processing is disabled
    or when the load method doesn't support versioning (e.g., OVERWRITE).

    Use cases:
    - Overwrite mode where table is fully replaced
    - Configurations with process_activate_version_messages=False
    """

    def activate_version(  # noqa: PLR6301
        self,
        table: sa.Table,  # noqa: ARG002
        context: LoadContext,  # noqa: ARG002
        new_version: int,
        engine: Engine,  # noqa: ARG002
    ) -> ActivateVersionResult:
        """No-op handler for ACTIVATE_VERSION.

        Args:
            table: SQLAlchemy Table object (unused).
            context: The load context (unused).
            new_version: The version being activated.
            engine: SQLAlchemy Engine (unused).

        Returns:
            ActivateVersionResult with just the version number.
        """
        return ActivateVersionResult(version_activated=new_version)
