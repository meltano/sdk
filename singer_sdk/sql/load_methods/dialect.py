"""Dialect helper protocol for database-specific operations."""

from __future__ import annotations

import typing as t

import sqlalchemy as sa

if t.TYPE_CHECKING:
    from sqlalchemy.engine import Engine
    from sqlalchemy.sql import Executable


@t.runtime_checkable
class DialectHelper(t.Protocol):
    """Protocol for dialect-specific database operations.

    This protocol abstracts operations that vary between database dialects,
    allowing load method strategies to work with different databases without
    tight coupling to a specific implementation.

    Implementations should be stateless and thread-safe.

    Example:
        >>> class PostgresDialectHelper:
        ...     def to_sql_type(self, jsonschema_type: dict) -> sa.types.TypeEngine:
        ...         # PostgreSQL-specific type mapping
        ...         ...
        ...
        ...     def generate_upsert_statement(self, table, records, key_columns):
        ...         # PostgreSQL ON CONFLICT syntax
        ...         ...
    """

    def to_sql_type(
        self,
        jsonschema_type: dict[str, t.Any],
    ) -> sa.types.TypeEngine:
        """Convert a JSON Schema type definition to a SQLAlchemy type.

        Args:
            jsonschema_type: A JSON Schema type definition dict. May include
                "type", "format", "maxLength", and other JSON Schema keywords.

        Returns:
            A SQLAlchemy TypeEngine instance appropriate for the dialect.

        Example:
            >>> helper.to_sql_type({"type": "string", "maxLength": 255})
            VARCHAR(255)
            >>> helper.to_sql_type({"type": "integer"})
            INTEGER()
        """
        ...

    def generate_upsert_statement(
        self,
        table: sa.Table,
        key_columns: t.Sequence[str],
    ) -> Executable:
        """Generate an upsert (INSERT ... ON CONFLICT/DUPLICATE) statement.

        Creates a database-specific upsert statement that inserts new records
        or updates existing ones based on key columns.

        Args:
            table: The SQLAlchemy Table object to upsert into.
            key_columns: Column names that form the unique key for matching.

        Returns:
            A SQLAlchemy executable statement that accepts record dicts
            as bind parameters.

        Example implementations by dialect:
            PostgreSQL: INSERT ... ON CONFLICT (keys) DO UPDATE SET ...
            MySQL: INSERT ... ON DUPLICATE KEY UPDATE ...
            SQLite: INSERT ... ON CONFLICT (keys) DO UPDATE SET ...
            Generic: Uses staging table pattern as fallback
        """
        ...

    def supports_native_upsert(self) -> bool:
        """Check if the dialect supports native upsert statements.

        Returns:
            True if the dialect supports INSERT ... ON CONFLICT or equivalent.
            False if upsert requires a staging table pattern.
        """
        ...


class GenericDialectHelper:
    """Generic dialect helper with sensible defaults.

    This implementation provides basic type mapping and uses a staging
    table pattern for upserts, making it compatible with most SQL databases.

    For better performance, use dialect-specific implementations that
    leverage native upsert capabilities.
    """

    # JSON Schema type to SQLAlchemy type mapping
    _type_mapping: t.ClassVar[dict[str, type[sa.types.TypeEngine]]] = {
        "string": sa.Text,
        "integer": sa.BigInteger,
        "number": sa.Numeric,
        "boolean": sa.Boolean,
        "object": sa.Text,  # JSON stored as text
        "array": sa.Text,  # JSON stored as text
    }

    def to_sql_type(
        self,
        jsonschema_type: dict[str, t.Any],
    ) -> sa.types.TypeEngine:
        """Convert JSON Schema type to SQLAlchemy type.

        Args:
            jsonschema_type: JSON Schema type definition.

        Returns:
            SQLAlchemy type instance.
        """
        # Handle nullable types (e.g., ["string", "null"])
        type_value = jsonschema_type.get("type", "string")
        if isinstance(type_value, list):
            # Filter out "null" and take first remaining type
            non_null_types = [t for t in type_value if t != "null"]
            type_value = non_null_types[0] if non_null_types else "string"

        # Handle format for specific types
        format_value = jsonschema_type.get("format", "")

        # Date/time formats
        if format_value == "date-time":
            return sa.DateTime()
        if format_value == "date":
            return sa.Date()
        if format_value == "time":
            return sa.Time()

        # String with maxLength becomes VARCHAR
        if type_value == "string":
            max_length = jsonschema_type.get("maxLength")
            if max_length:
                return sa.String(max_length)
            return sa.Text()

        # Look up base type
        sql_type_class = self._type_mapping.get(type_value, sa.Text)
        return sql_type_class()

    def generate_upsert_statement(  # noqa: PLR6301
        self,
        table: sa.Table,
        key_columns: t.Sequence[str],  # noqa: ARG002
    ) -> Executable:
        """Generate a simple INSERT statement.

        The generic helper doesn't support native upsert, so this returns
        a plain INSERT. Use `supports_native_upsert()` to check and fall
        back to staging table pattern when False.

        Args:
            table: Target table.
            key_columns: Key columns (unused in generic implementation).

        Returns:
            A simple INSERT statement.
        """
        return sa.insert(table)

    def supports_native_upsert(self) -> bool:  # noqa: PLR6301
        """Generic helper does not support native upsert.

        Returns:
            False - requires staging table pattern for upsert.
        """
        return False


class SQLiteDialectHelper(GenericDialectHelper):
    """SQLite-specific dialect helper.

    Supports SQLite's INSERT ... ON CONFLICT syntax (SQLite 3.24+).
    """

    def generate_upsert_statement(  # noqa: PLR6301
        self,
        table: sa.Table,
        key_columns: t.Sequence[str],
    ) -> Executable:
        """Generate SQLite upsert using ON CONFLICT.

        Args:
            table: Target table.
            key_columns: Columns for conflict detection.

        Returns:
            INSERT ... ON CONFLICT DO UPDATE statement.
        """
        from sqlalchemy.dialects.sqlite import (  # noqa: PLC0415
            insert as sqlite_insert,
        )

        insert_stmt = sqlite_insert(table)

        # Build the SET clause for non-key columns
        update_dict = {
            col.name: insert_stmt.excluded[col.name]
            for col in table.columns
            if col.name not in key_columns
        }

        if update_dict:
            return insert_stmt.on_conflict_do_update(
                index_elements=list(key_columns),
                set_=update_dict,
            )
        # If only key columns, just ignore conflicts
        return insert_stmt.on_conflict_do_nothing(
            index_elements=list(key_columns),
        )

    def supports_native_upsert(self) -> bool:  # noqa: PLR6301
        """SQLite supports ON CONFLICT syntax.

        Returns:
            True.
        """
        return True


class PostgreSQLDialectHelper(GenericDialectHelper):
    """PostgreSQL-specific dialect helper.

    Supports PostgreSQL's INSERT ... ON CONFLICT syntax.
    """

    def to_sql_type(
        self,
        jsonschema_type: dict[str, t.Any],
    ) -> sa.types.TypeEngine:
        """Convert JSON Schema type to PostgreSQL type.

        Uses JSONB for object/array types instead of Text.

        Args:
            jsonschema_type: JSON Schema type definition.

        Returns:
            SQLAlchemy type instance.
        """
        type_value = jsonschema_type.get("type", "string")
        if isinstance(type_value, list):
            non_null_types = [t for t in type_value if t != "null"]
            type_value = non_null_types[0] if non_null_types else "string"

        # Use JSONB for object/array types
        if type_value in {"object", "array"}:
            from sqlalchemy.dialects.postgresql import (  # noqa: PLC0415
                JSONB,
            )

            return JSONB()

        return super().to_sql_type(jsonschema_type)

    def generate_upsert_statement(  # noqa: PLR6301
        self,
        table: sa.Table,
        key_columns: t.Sequence[str],
    ) -> Executable:
        """Generate PostgreSQL upsert using ON CONFLICT.

        Args:
            table: Target table.
            key_columns: Columns for conflict detection.

        Returns:
            INSERT ... ON CONFLICT DO UPDATE statement.
        """
        from sqlalchemy.dialects.postgresql import (  # noqa: PLC0415
            insert as pg_insert,
        )

        insert_stmt = pg_insert(table)

        update_dict = {
            col.name: insert_stmt.excluded[col.name]
            for col in table.columns
            if col.name not in key_columns
        }

        if update_dict:
            return insert_stmt.on_conflict_do_update(
                index_elements=list(key_columns),
                set_=update_dict,
            )
        return insert_stmt.on_conflict_do_nothing(
            index_elements=list(key_columns),
        )

    def supports_native_upsert(self) -> bool:  # noqa: PLR6301
        """PostgreSQL supports ON CONFLICT syntax.

        Returns:
            True.
        """
        return True


class MySQLDialectHelper(GenericDialectHelper):
    """MySQL-specific dialect helper.

    Supports MySQL's INSERT ... ON DUPLICATE KEY UPDATE syntax.
    """

    def generate_upsert_statement(  # noqa: PLR6301
        self,
        table: sa.Table,
        key_columns: t.Sequence[str],
    ) -> Executable:
        """Generate MySQL upsert using ON DUPLICATE KEY UPDATE.

        Args:
            table: Target table.
            key_columns: Columns for conflict detection (implicit in MySQL).

        Returns:
            INSERT ... ON DUPLICATE KEY UPDATE statement.
        """
        from sqlalchemy.dialects.mysql import (  # noqa: PLC0415
            insert as mysql_insert,
        )

        insert_stmt = mysql_insert(table)

        update_dict = {
            col.name: insert_stmt.inserted[col.name]
            for col in table.columns
            if col.name not in key_columns
        }

        if update_dict:
            return insert_stmt.on_duplicate_key_update(**update_dict)
        return insert_stmt

    def supports_native_upsert(self) -> bool:  # noqa: PLR6301
        """MySQL supports ON DUPLICATE KEY UPDATE syntax.

        Returns:
            True.
        """
        return True


def get_dialect_helper(engine: Engine) -> DialectHelper:
    """Get the appropriate dialect helper for an engine.

    Factory function that returns a dialect-specific helper based on
    the engine's dialect name.

    Args:
        engine: SQLAlchemy Engine instance.

    Returns:
        A DialectHelper appropriate for the engine's dialect.

    Example:
        >>> engine = sa.create_engine("postgresql://...")
        >>> helper = get_dialect_helper(engine)
        >>> isinstance(helper, PostgreSQLDialectHelper)
        True
    """
    dialect_name = engine.dialect.name

    if dialect_name == "sqlite":
        return SQLiteDialectHelper()
    if dialect_name == "postgresql":
        return PostgreSQLDialectHelper()
    if dialect_name in {"mysql", "mariadb"}:
        return MySQLDialectHelper()

    # Fall back to generic helper
    return GenericDialectHelper()
