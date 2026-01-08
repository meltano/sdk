"""Data loaders for SQL sinks.

This module implements the Loader pattern for different data loading approaches,
separating DML (data loading) concerns from DDL (table preparation) concerns.

Loaders can be instantiated standalone with minimal dependencies (just a
SQLAlchemy engine and schema information), making them easy to test and reuse.

Developers can subclass these loaders to customize loading behavior without
modifying the strategy classes.
"""

from __future__ import annotations

import abc
import logging
import typing as t
import uuid
from contextlib import contextmanager

import sqlalchemy as sa

if t.TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Sequence

__all__ = [
    "Loader",
    "MergeUpsertLoader",
    "SimpleInsertLoader",
    "TempTableUpsertLoader",
]


class Loader(abc.ABC):
    """Abstract base class for data loaders.

    Loaders handle the actual mechanics of loading data (DML operations).
    They can be instantiated with minimal dependencies:
    - SQLAlchemy engine/connection
    - Schema information
    - Column name conforming function

    This separation allows developers to:
    1. Use loaders standalone without full sink/connector setup
    2. Test loaders in isolation
    3. Customize loading behavior by subclassing
    4. Share loaders across different frameworks
    """

    def __init__(
        self,
        engine: sa.engine.Engine,
        schema: dict,
        key_properties: Sequence[str] | None = None,
        *,
        conform_name: Callable[[str, str], str] | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        """Initialize the loader.

        Args:
            engine: SQLAlchemy engine for database connections.
            schema: JSON schema describing the table structure.
            key_properties: List of primary key column names (required for upsert).
            conform_name: Optional function to conform column names.
                Signature: conform_name(name: str, object_type: str) -> str
                If not provided, names are used as-is.
            logger: Optional logger instance. If not provided, uses a default logger.
        """
        self.engine = engine
        self.schema = schema
        self.key_properties = key_properties or []
        self.conform_name = conform_name or self._default_conform_name
        self.logger = logger or logging.getLogger(__name__)

    @staticmethod
    def _default_conform_name(name: str, object_type: str) -> str:  # noqa: ARG004
        """Default name conforming function (identity).

        Args:
            name: The name to conform.
            object_type: The type of object ('table', 'column', etc.).

        Returns:
            The name unchanged.
        """
        return name

    @contextmanager
    def _connect(self) -> Iterator[sa.engine.Connection]:
        """Get a database connection.

        Yields:
            A SQLAlchemy connection.
        """
        with self.engine.connect() as conn:
            yield conn

    @abc.abstractmethod
    def load_records(
        self,
        full_table_name: str,
        schema: dict,
        records: t.Iterable[dict[str, t.Any]],
    ) -> int | None:
        """Load records to the target table.

        Args:
            full_table_name: The fully qualified table name.
            schema: The JSON schema for the table.
            records: The records to load.

        Returns:
            The number of records loaded, or None if unknown.
        """
        ...


class SimpleInsertLoader(Loader):
    """Loader that uses simple INSERT statements.

    This is the simplest and most compatible loader:
    - Always INSERTs records, never updates
    - Works with any SQL database
    - No special database capabilities required
    - Records can be inserted multiple times (duplicates allowed)

    Use this for:
    - Append-only loading
    - Overwrite loading (after table is cleared)

    Example:
        >>> engine = create_engine("sqlite:///mydb.db")  # doctest: +SKIP
        >>> schema = {
        ...     "properties": {"id": {"type": "string"}, "name": {"type": "string"}}
        ... }  # doctest: +SKIP
        >>> loader = SimpleInsertLoader(engine, schema)  # doctest: +SKIP
        >>> loader.load_records(
        ...     "users", schema, [{"id": "1", "name": "Alice"}]
        ... )  # doctest: +SKIP
    """

    def load_records(
        self,
        full_table_name: str,
        schema: dict,
        records: t.Iterable[dict[str, t.Any]],
    ) -> int | None:
        """Load records using simple INSERT.

        Args:
            full_table_name: The fully qualified table name.
            schema: The JSON schema for the table.
            records: The records to load.

        Returns:
            The number of records inserted.
        """
        records_list = list(records)
        if not records_list:
            return 0

        # Build INSERT statement
        insert_stmt = self._build_insert_statement(full_table_name, schema)

        # Execute bulk insert
        with self._connect() as conn:
            result = conn.execute(insert_stmt, records_list)
            conn.commit()
            return result.rowcount if result.rowcount >= 0 else None

    def _build_insert_statement(
        self,
        full_table_name: str,
        schema: dict,
    ) -> sa.sql.Insert:
        """Build INSERT statement for the table.

        Args:
            full_table_name: The fully qualified table name.
            schema: The JSON schema for the table.

        Returns:
            A SQLAlchemy INSERT statement.
        """
        # Parse table name
        table_name = full_table_name.rsplit(".", maxsplit=1)[-1]
        schema_name = (
            ".".join(full_table_name.split(".")[:-1])
            if "." in full_table_name
            else None
        )

        # Get column names
        columns = [self.conform_name(prop, "column") for prop in schema["properties"]]

        # Build table metadata
        meta = sa.MetaData(schema=schema_name)
        table = sa.Table(
            table_name,
            meta,
            *[sa.Column(col) for col in columns],
        )

        # Build INSERT statement
        return sa.insert(table)


class TempTableUpsertLoader(Loader):
    """Loader that uses temp tables for upsert operations.

    This is a generic upsert implementation that works on most SQL databases:
    1. Create temp staging table
    2. Bulk INSERT records into temp
    3. DELETE from target WHERE keys match temp
    4. INSERT into target SELECT * FROM temp
    5. DROP temp table

    Requires:
    - Primary keys must be defined
    - Database must support temp tables

    Use this for:
    - Generic upsert when no database-specific MERGE is available
    - Databases that support temp tables (most SQL databases)

    Example:
        >>> engine = create_engine("sqlite:///mydb.db")  # doctest: +SKIP
        >>> schema = {
        ...     "properties": {"id": {"type": "string"}, "name": {"type": "string"}}
        ... }  # doctest: +SKIP
        >>> loader = TempTableUpsertLoader(  # doctest: +SKIP
        ...     engine,  # doctest: +SKIP
        ...     schema,  # doctest: +SKIP
        ...     key_properties=["id"],  # doctest: +SKIP
        ...     temp_table_creator=create_sqlite_temp_table,  # doctest: +SKIP
        ... )  # doctest: +SKIP
        >>> loader.load_records(
        ...     "users", schema, [{"id": "1", "name": "Alice"}]
        ... )  # doctest: +SKIP
    """

    def __init__(
        self,
        engine: sa.engine.Engine,
        schema: dict,
        key_properties: Sequence[str] | None = None,
        *,
        conform_name: Callable[[str, str], str] | None = None,
        logger: logging.Logger | None = None,
        temp_table_creator: Callable[[str, dict, sa.engine.Engine], None] | None = None,
    ) -> None:
        """Initialize the temp table upsert loader.

        Args:
            engine: SQLAlchemy engine for database connections.
            schema: JSON schema describing the table structure.
            key_properties: List of primary key column names (required for upsert).
            conform_name: Optional function to conform column names.
            logger: Optional logger instance.
            temp_table_creator: Optional function to create temp tables.
                Signature: (table_name: str, schema: dict, engine: Engine) -> None
                If not provided, uses a generic implementation.
        """
        super().__init__(
            engine, schema, key_properties, conform_name=conform_name, logger=logger
        )
        self.temp_table_creator = temp_table_creator or self._create_temp_table_default

    def _create_temp_table_default(
        self,
        temp_table_name: str,
        schema: dict,
        engine: sa.engine.Engine,
    ) -> None:
        """Default temp table creation implementation.

        Args:
            temp_table_name: The temp table name.
            schema: The JSON schema for the table.
            engine: SQLAlchemy engine.
        """
        # Parse table name
        table_name = temp_table_name.rsplit(".", maxsplit=1)[-1]
        schema_name = (
            ".".join(temp_table_name.split(".")[:-1])
            if "." in temp_table_name
            else None
        )

        # Get column names
        columns = [self.conform_name(prop, "column") for prop in schema["properties"]]

        # Build table metadata
        meta = sa.MetaData(schema=schema_name)
        table = sa.Table(
            table_name,
            meta,
            *[sa.Column(col) for col in columns],
            prefixes=["TEMPORARY"],
        )

        # Create temp table
        table.create(engine, checkfirst=True)

    def load_records(
        self,
        full_table_name: str,
        schema: dict,
        records: t.Iterable[dict[str, t.Any]],
    ) -> int | None:
        """Load records using temp-table-based upsert.

        Args:
            full_table_name: The fully qualified table name.
            schema: The JSON schema for the table.
            records: The records to load.

        Returns:
            The number of records upserted.
        """
        # Convert to list to allow multiple iterations
        records_list = list(records)

        if not records_list:
            return 0

        # Create temp table for staging
        temp_table_name = self._create_temp_table_name(full_table_name)

        # Create temp table with same schema
        self.temp_table_creator(temp_table_name, schema, self.engine)

        try:
            # Step 1: Bulk insert into temp table
            insert_stmt = self._build_insert_statement(temp_table_name, schema)
            with self._connect() as conn:
                conn.execute(insert_stmt, records_list)
                conn.commit()

            # Step 2: Merge from temp to target using DELETE + INSERT pattern
            return self._merge_from_temp(
                target_table_name=full_table_name,
                temp_table_name=temp_table_name,
                schema=schema,
            )

        finally:
            # Clean up temp table
            self._drop_temp_table(temp_table_name)

    def _build_insert_statement(
        self,
        full_table_name: str,
        schema: dict,
    ) -> sa.sql.Insert:
        """Build INSERT statement for the table.

        Args:
            full_table_name: The fully qualified table name.
            schema: The JSON schema for the table.

        Returns:
            A SQLAlchemy INSERT statement.
        """
        # Parse table name
        table_name = full_table_name.rsplit(".", maxsplit=1)[-1]
        schema_name = (
            ".".join(full_table_name.split(".")[:-1])
            if "." in full_table_name
            else None
        )

        # Get column names
        columns = [self.conform_name(prop, "column") for prop in schema["properties"]]

        # Build table metadata
        meta = sa.MetaData(schema=schema_name)
        table = sa.Table(
            table_name,
            meta,
            *[sa.Column(col) for col in columns],
        )

        # Build INSERT statement
        return sa.insert(table)

    def _merge_from_temp(
        self,
        target_table_name: str,
        temp_table_name: str,
        schema: dict,
    ) -> int | None:
        """Merge data from temp table to target using DELETE + INSERT.

        Args:
            target_table_name: The target table.
            temp_table_name: The temp staging table.
            schema: The JSON schema.

        Returns:
            The number of records upserted.
        """
        # Parse table names
        target_table = target_table_name.rsplit(".", maxsplit=1)[-1]
        target_schema_name = (
            ".".join(target_table_name.split(".")[:-1])
            if "." in target_table_name
            else None
        )

        temp_table = temp_table_name.rsplit(".", maxsplit=1)[-1]
        temp_schema_name = (
            ".".join(temp_table_name.split(".")[:-1])
            if "." in temp_table_name
            else None
        )

        # Build qualified table names
        target_fqn = (
            f"{target_schema_name}.{target_table}"
            if target_schema_name
            else target_table
        )
        temp_fqn = (
            f"{temp_schema_name}.{temp_table}" if temp_schema_name else temp_table
        )

        # Get conformed column names
        key_columns = [self.conform_name(key, "column") for key in self.key_properties]
        all_columns = [
            self.conform_name(prop, "column") for prop in schema["properties"]
        ]

        with self._connect() as conn, conn.begin():
            # Step 1: DELETE existing records that match keys in temp
            delete_sql = self._build_delete_sql(target_fqn, temp_fqn, key_columns)
            self.logger.info("Deleting existing records: %s", delete_sql)
            conn.execute(sa.text(delete_sql))

            # Step 2: INSERT all records from temp into target
            insert_sql = self._build_insert_from_temp_sql(
                target_fqn, temp_fqn, all_columns
            )
            self.logger.info("Inserting merged records: %s", insert_sql)
            result = conn.execute(sa.text(insert_sql))

            return result.rowcount if result.rowcount >= 0 else None

    def _build_delete_sql(  # noqa: PLR6301
        self,
        target_table: str,
        temp_table: str,
        key_columns: list[str],
    ) -> str:
        """Build DELETE SQL for removing existing records.

        Args:
            target_table: The target table name.
            temp_table: The temp table name.
            key_columns: The primary key column names.

        Returns:
            The DELETE SQL statement.
        """
        if len(key_columns) == 1:
            # Simple case: single key
            key_col = key_columns[0]
            return (
                f"DELETE FROM {target_table} "  # noqa: S608
                f"WHERE {key_col} IN (SELECT {key_col} FROM {temp_table})"
            )

        # Multiple keys: use tuple comparison
        keys_tuple = ", ".join(key_columns)
        return (
            f"DELETE FROM {target_table} "  # noqa: S608
            f"WHERE ({keys_tuple}) IN (SELECT {keys_tuple} FROM {temp_table})"
        )

    def _build_insert_from_temp_sql(  # noqa: PLR6301
        self,
        target_table: str,
        temp_table: str,
        columns: list[str],
    ) -> str:
        """Build INSERT...SELECT SQL for loading from temp.

        Args:
            target_table: The target table name.
            temp_table: The temp table name.
            columns: The column names to insert.

        Returns:
            The INSERT...SELECT SQL statement.
        """
        columns_list = ", ".join(columns)
        return (
            f"INSERT INTO {target_table} ({columns_list}) "  # noqa: S608
            f"SELECT {columns_list} FROM {temp_table}"
        )

    def _create_temp_table_name(self, base_table_name: str) -> str:
        """Generate a unique temp table name.

        Args:
            base_table_name: The base table name.

        Returns:
            A unique temp table name.
        """
        # Parse to get just the table name without schema
        table_name = base_table_name.rsplit(".", maxsplit=1)[-1]

        # Generate unique suffix
        unique_suffix = str(uuid.uuid4()).replace("-", "")[:8]

        return f"{table_name}_temp_{unique_suffix}"

    def _drop_temp_table(self, temp_table_name: str) -> None:
        """Drop the temp table.

        Args:
            temp_table_name: The temp table name to drop.
        """
        try:
            table_name = temp_table_name.rsplit(".", maxsplit=1)[-1]
            schema_name = (
                ".".join(temp_table_name.split(".")[:-1])
                if "." in temp_table_name
                else None
            )

            meta = sa.MetaData()
            table = sa.Table(table_name, meta, schema=schema_name)
            table.drop(self.engine, checkfirst=True)
            self.logger.debug("Dropped temp table %s", temp_table_name)
        except Exception as ex:  # noqa: BLE001
            # Log but don't fail if temp table cleanup fails
            self.logger.warning(
                "Failed to drop temp table %s: %s",
                temp_table_name,
                ex,
            )


class MergeUpsertLoader(Loader):
    """Loader that uses database-specific MERGE/UPSERT statements.

    This loader uses a custom merge function for database-specific MERGE/UPSERT SQL.

    Requires:
    - A merge function that implements the database-specific merge logic
    - Database must support temp tables (for staging)

    Use this for:
    - Databases with native MERGE support (SQL Server, Oracle, etc.)
    - Databases with UPSERT extensions (PostgreSQL ON CONFLICT, SQLite UPSERT)
    - Custom high-performance merge implementations

    Example:
        >>> def postgres_merge(target, temp, keys, conn):  # doctest: +SKIP
        ...     # PostgreSQL INSERT ... ON CONFLICT  # doctest: +SKIP
        ...     return 10  # number of rows affected  # doctest: +SKIP
        >>> engine = create_engine("postgresql://...")  # doctest: +SKIP
        >>> schema = {
        ...     "properties": {"id": {"type": "string"}, "name": {"type": "string"}}
        ... }  # doctest: +SKIP
        >>> loader = MergeUpsertLoader(  # doctest: +SKIP
        ...     engine,  # doctest: +SKIP
        ...     schema,  # doctest: +SKIP
        ...     key_properties=["id"],  # doctest: +SKIP
        ...     merge_function=postgres_merge,  # doctest: +SKIP
        ... )  # doctest: +SKIP
    """

    def __init__(
        self,
        engine: sa.engine.Engine,
        schema: dict,
        key_properties: Sequence[str] | None = None,
        *,
        conform_name: Callable[[str, str], str] | None = None,
        logger: logging.Logger | None = None,
        temp_table_creator: Callable[[str, dict, sa.engine.Engine], None] | None = None,
        merge_function: Callable[
            [str, str, list[str], sa.engine.Connection], int | None
        ]
        | None = None,
    ) -> None:
        """Initialize the merge upsert loader.

        Args:
            engine: SQLAlchemy engine for database connections.
            schema: JSON schema describing the table structure.
            key_properties: List of primary key column names (required for upsert).
            conform_name: Optional function to conform column names.
            logger: Optional logger instance.
            temp_table_creator: Optional function to create temp tables.
            merge_function: Function to perform the merge.
                Signature: (target_table: str, temp_table: str, keys: list[str], conn: Connection) -> int | None
                Returns the number of rows affected.
        """
        super().__init__(
            engine, schema, key_properties, conform_name=conform_name, logger=logger
        )
        self.temp_table_creator = temp_table_creator or self._create_temp_table_default
        if not merge_function:
            msg = "MergeUpsertLoader requires a merge_function"
            raise ValueError(msg)
        self.merge_function: Callable[
            [str, str, list[str], sa.engine.Connection], int | None
        ] = merge_function

    def _create_temp_table_default(
        self,
        temp_table_name: str,
        schema: dict,
        engine: sa.engine.Engine,
    ) -> None:
        """Default temp table creation implementation.

        Args:
            temp_table_name: The temp table name.
            schema: The JSON schema for the table.
            engine: SQLAlchemy engine.
        """
        # Parse table name
        table_name = temp_table_name.rsplit(".", maxsplit=1)[-1]
        schema_name = (
            ".".join(temp_table_name.split(".")[:-1])
            if "." in temp_table_name
            else None
        )

        # Get column names
        columns = [self.conform_name(prop, "column") for prop in schema["properties"]]

        # Build table metadata
        meta = sa.MetaData(schema=schema_name)
        table = sa.Table(
            table_name,
            meta,
            *[sa.Column(col) for col in columns],
            prefixes=["TEMPORARY"],
        )

        # Create temp table
        table.create(engine, checkfirst=True)

    def load_records(
        self,
        full_table_name: str,
        schema: dict,
        records: t.Iterable[dict[str, t.Any]],
    ) -> int | None:
        """Load records using custom merge function.

        Args:
            full_table_name: The fully qualified table name.
            schema: The JSON schema for the table.
            records: The records to load.

        Returns:
            The number of records merged.
        """
        # Convert to list
        records_list = list(records)

        if not records_list:
            return 0

        # Create temp table for staging
        temp_table_name = self._create_temp_table_name(full_table_name)

        # Create temp table with same schema as target
        self.temp_table_creator(temp_table_name, schema, self.engine)

        try:
            # Bulk insert into temp table
            insert_stmt = self._build_insert_statement(temp_table_name, schema)
            with self._connect() as conn:
                conn.execute(insert_stmt, records_list)
                conn.commit()

                # Call custom merge implementation
                return self.merge_function(
                    full_table_name,
                    temp_table_name,
                    list(self.key_properties),
                    conn,
                )

        finally:
            # Clean up temp table
            self._drop_temp_table(temp_table_name)

    def _build_insert_statement(
        self,
        full_table_name: str,
        schema: dict,
    ) -> sa.sql.Insert:
        """Build INSERT statement for the table.

        Args:
            full_table_name: The fully qualified table name.
            schema: The JSON schema for the table.

        Returns:
            A SQLAlchemy INSERT statement.
        """
        # Parse table name
        table_name = full_table_name.rsplit(".", maxsplit=1)[-1]
        schema_name = (
            ".".join(full_table_name.split(".")[:-1])
            if "." in full_table_name
            else None
        )

        # Get column names
        columns = [self.conform_name(prop, "column") for prop in schema["properties"]]

        # Build table metadata
        meta = sa.MetaData(schema=schema_name)
        table = sa.Table(
            table_name,
            meta,
            *[sa.Column(col) for col in columns],
        )

        # Build INSERT statement
        return sa.insert(table)

    def _create_temp_table_name(self, base_table_name: str) -> str:
        """Generate a unique temp table name.

        Args:
            base_table_name: The base table name.

        Returns:
            A unique temp table name.
        """
        table_name = base_table_name.rsplit(".", maxsplit=1)[-1]
        unique_suffix = str(uuid.uuid4()).replace("-", "")[:8]
        return f"{table_name}_temp_{unique_suffix}"

    def _drop_temp_table(self, temp_table_name: str) -> None:
        """Drop the temp table.

        Args:
            temp_table_name: The temp table name to drop.
        """
        try:
            table_name = temp_table_name.rsplit(".", maxsplit=1)[-1]
            schema_name = (
                ".".join(temp_table_name.split(".")[:-1])
                if "." in temp_table_name
                else None
            )

            meta = sa.MetaData()
            table = sa.Table(table_name, meta, schema=schema_name)
            table.drop(self.engine, checkfirst=True)
            self.logger.debug("Dropped temp table %s", temp_table_name)
        except Exception as ex:  # noqa: BLE001
            self.logger.warning(
                "Failed to drop temp table %s: %s",
                temp_table_name,
                ex,
            )
