"""Load method strategies for SQL sinks.

This module implements the Strategy pattern for different SQL load methods
(append-only, upsert, overwrite), following SOLID principles to make the
load logic easier to implement, override, and maintain.

Strategies handle:
- Table preparation (DDL)
- Configuration validation
- Strategy orchestration

Loaders handle:
- Data loading (DML)
- Record insertion/upsert logic

This separation allows developers to customize loading behavior by
subclassing loaders without modifying strategy classes.
"""

from __future__ import annotations

import abc
import typing as t

import sqlalchemy as sa

from singer_sdk.sql.loaders import (
    MergeUpsertLoader,
    SimpleInsertLoader,
    TempTableUpsertLoader,
)

if t.TYPE_CHECKING:
    from singer_sdk.sql import SQLConnector, SQLSink
    from singer_sdk.sql.loaders import Loader

__all__ = [
    "AppendOnlyStrategy",
    "LoadMethodStrategy",
    "OverwriteStrategy",
    "UpsertStrategy",
]


class LoadMethodStrategy(abc.ABC):
    """Abstract base class for load method strategies.

    Strategies orchestrate the overall load method behavior:
    - Table preparation (DDL operations like CREATE TABLE, ADD COLUMN)
    - Configuration validation
    - Selecting and configuring the appropriate Loader

    Loaders handle the actual data loading (DML operations).

    This separation follows SOLID principles:
    - Single Responsibility: Strategies handle DDL, Loaders handle DML
    - Open/Closed: Developers can extend by subclassing Loaders
    - Dependency Inversion: Strategies depend on Loader abstraction

    To customize loading behavior, subclass the appropriate Loader rather than
    modifying the strategy classes.
    """

    def __init__(
        self,
        connector: SQLConnector,
        sink: SQLSink,
    ) -> None:
        """Initialize the load strategy.

        Args:
            connector: The SQL connector instance.
            sink: The SQL sink instance.
        """
        self.connector = connector
        self.sink = sink
        self.logger = sink.logger
        self.loader: Loader = self._create_loader()

    @abc.abstractmethod
    def _create_loader(self) -> Loader:
        """Create the appropriate loader for this strategy.

        Returns:
            A Loader instance that handles data loading for this strategy.
        """
        ...

    @abc.abstractmethod
    def prepare_table(
        self,
        full_table_name: str,
        schema: dict,
        primary_keys: t.Sequence[str],
    ) -> None:
        """Prepare the target table for loading.

        This method handles DDL operations like CREATE TABLE, ADD COLUMN, etc.

        Args:
            full_table_name: The fully qualified table name.
            schema: The JSON schema for the table.
            primary_keys: List of primary key column names.
        """
        ...

    def load_batch(
        self,
        full_table_name: str,
        schema: dict,
        records: t.Iterable[dict[str, t.Any]],
    ) -> int | None:
        """Load a batch of records to the target table.

        This method delegates to the loader for DML operations.

        Developers can customize loading behavior by:
        1. Subclassing the appropriate Loader (e.g., TempTableUpsertLoader)
        2. Overriding _create_loader() to return the custom loader instance

        Args:
            full_table_name: The fully qualified table name.
            schema: The JSON schema for the table.
            records: The records to load.

        Returns:
            The number of records loaded, or None if unknown.
        """
        return self.loader.load_records(full_table_name, schema, records)

    @abc.abstractmethod
    def validate_config(self) -> None:
        """Validate that the connector supports this load method.

        Raises:
            ValueError: If the load method is not supported by the connector.
        """
        ...


class AppendOnlyStrategy(LoadMethodStrategy):
    """Strategy for append-only loading.

    This is the default and simplest load method:
    - Tables are created if they don't exist
    - Schema evolution adds new columns incrementally
    - Records are always INSERTed, never updated or deleted
    - No special database capabilities required

    Works with any SQL database.

    Uses SimpleInsertLoader for data loading.
    """

    def _create_loader(self) -> Loader:
        """Create a SimpleInsertLoader.

        Returns:
            A SimpleInsertLoader instance.
        """
        return SimpleInsertLoader(
            engine=self.connector._engine,  # noqa: SLF001
            schema=self.sink.schema,
            key_properties=self.sink.key_properties,
            conform_name=self.sink.conform_name,
            logger=self.logger,
        )

    def validate_config(self) -> None:
        """Validate configuration.

        Append-only has no special requirements - works everywhere.
        """

    def prepare_table(
        self,
        full_table_name: str,
        schema: dict,
        primary_keys: t.Sequence[str],
    ) -> None:
        """Prepare table for append-only loading.

        If table doesn't exist, creates it.
        If table exists, adds any new columns incrementally.

        Args:
            full_table_name: The fully qualified table name.
            schema: The JSON schema for the table.
            primary_keys: List of primary key column names.
        """
        # Check if table exists
        if not self.connector.table_exists(full_table_name):
            # Create new table
            self.logger.info("Creating table %s", full_table_name)
            self.connector.create_empty_table(
                full_table_name=full_table_name,
                schema=schema,
                primary_keys=primary_keys,
                as_temp_table=False,
            )
            return

        # Table exists - add any new columns
        for property_name, property_def in schema["properties"].items():
            self.connector.prepare_column(
                full_table_name,
                property_name,
                self.connector.to_sql_type(property_def),
            )

        # Ensure primary keys are set
        self.connector.prepare_primary_key(
            full_table_name=full_table_name,
            primary_keys=primary_keys,
        )


class OverwriteStrategy(LoadMethodStrategy):
    """Strategy for overwrite loading.

    Overwrite mode:
    - On first sync: DROP TABLE IF EXISTS, then CREATE TABLE
    - Insert all records
    - Subsequent schema changes: Add columns incrementally (don't drop again)

    Requires: connector.allow_overwrite = True

    Uses SimpleInsertLoader for data loading (table is already cleared).
    """

    def _create_loader(self) -> Loader:
        """Create a SimpleInsertLoader.

        Returns:
            A SimpleInsertLoader instance.
        """
        return SimpleInsertLoader(
            engine=self.connector._engine,  # noqa: SLF001
            schema=self.sink.schema,
            key_properties=self.sink.key_properties,
            conform_name=self.sink.conform_name,
            logger=self.logger,
        )

    def validate_config(self) -> None:
        """Validate that overwrite is supported.

        Raises:
            ValueError: If overwrite is not allowed by the connector.
        """
        if not self.connector.allow_overwrite:
            msg = (
                "OVERWRITE load method is not supported by "
                f"{self.connector.__class__.__name__}. "
                "Set allow_overwrite = True or use a different load method."
            )
            raise ValueError(msg)

    def prepare_table(
        self,
        full_table_name: str,
        schema: dict,
        primary_keys: t.Sequence[str],
    ) -> None:
        """Prepare table for overwrite loading.

        On first preparation: DROP + CREATE table
        On subsequent preparations: Add columns incrementally

        Args:
            full_table_name: The fully qualified table name.
            schema: The JSON schema for the table.
            primary_keys: List of primary key column names.
        """
        # Track if this table has been prepared already in this run
        table_key = str(full_table_name)
        table_already_prepared = self.connector._tables_prepared.get(table_key, False)  # noqa: SLF001

        # Only drop/recreate on FIRST preparation per target run
        # This prevents data loss when schema changes mid-stream
        if not table_already_prepared or not self.connector.table_exists(
            full_table_name
        ):
            self.logger.info(
                "Dropping and recreating table %s (OVERWRITE mode)", full_table_name
            )

            # Parse table name components
            _, schema_name, table_name = self.connector.parse_full_table_name(
                full_table_name
            )

            # DROP TABLE IF EXISTS
            meta = sa.MetaData()
            table = sa.Table(table_name, meta, schema=schema_name)
            table.drop(self.connector._engine, checkfirst=True)  # noqa: SLF001

            # CREATE TABLE
            self.connector.create_empty_table(
                full_table_name=full_table_name,
                schema=schema,
                primary_keys=primary_keys,
                as_temp_table=False,
            )

            # Mark as prepared
            self.connector._tables_prepared[table_key] = True  # noqa: SLF001
            return

        # Table already prepared and exists - add columns incrementally
        for property_name, property_def in schema["properties"].items():
            self.connector.prepare_column(
                full_table_name,
                property_name,
                self.connector.to_sql_type(property_def),
            )

        # Ensure primary keys are set
        self.connector.prepare_primary_key(
            full_table_name=full_table_name,
            primary_keys=primary_keys,
        )

        # Update prepared state
        self.connector._tables_prepared[table_key] = True  # noqa: SLF001


class UpsertStrategy(LoadMethodStrategy):
    """Strategy for upsert (update + insert) loading.

    Upsert mode:
    - Updates existing records based on primary keys
    - Inserts new records that don't exist
    - Automatically chooses the appropriate loader:
      * MergeUpsertLoader: If custom merge_upsert_from_table() is implemented
      * TempTableUpsertLoader: For generic temp-table-based upsert

    Requires:
    - Primary keys must be defined
    - connector.allow_temp_tables = True (for TempTableUpsertLoader)
    - OR connector.allow_merge_upsert = True (for MergeUpsertLoader)

    To customize upsert behavior, subclass TempTableUpsertLoader or
    MergeUpsertLoader and override _create_loader() to return your custom loader.
    """

    def _create_loader(self) -> Loader:
        """Create the appropriate upsert loader.

        Chooses between MergeUpsertLoader (for custom merge) and
        TempTableUpsertLoader (for generic upsert).

        Returns:
            A Loader instance for upsert operations.
        """
        # Check if custom merge_upsert_from_table is implemented
        has_custom_merge = (
            type(self.sink).merge_upsert_from_table
            != t.cast("t.Any", self.sink).__class__.__bases__[0].merge_upsert_from_table
        )

        # Create temp table creator function
        def temp_table_creator(
            temp_table_name: str,
            schema: dict,
            engine: sa.engine.Engine,
        ) -> None:
            self.connector.create_empty_table(
                full_table_name=temp_table_name,
                schema=schema,
                primary_keys=self.sink.key_properties,
                as_temp_table=True,
            )

        if has_custom_merge:
            # Create merge function wrapper
            def merge_function(
                target_table: str,
                temp_table: str,
                keys: list[str],
                conn: sa.engine.Connection,
            ) -> int | None:
                return self.sink.merge_upsert_from_table(
                    target_table_name=target_table,
                    from_table_name=temp_table,
                    join_keys=keys,
                )

            # Use custom merge implementation
            return MergeUpsertLoader(
                engine=self.connector._engine,  # noqa: SLF001
                schema=self.sink.schema,
                key_properties=self.sink.key_properties,
                conform_name=self.sink.conform_name,
                logger=self.logger,
                temp_table_creator=temp_table_creator,
                merge_function=merge_function,
            )

        # Use generic temp-table-based upsert
        return TempTableUpsertLoader(
            engine=self.connector._engine,  # noqa: SLF001
            schema=self.sink.schema,
            key_properties=self.sink.key_properties,
            conform_name=self.sink.conform_name,
            logger=self.logger,
            temp_table_creator=temp_table_creator,
        )

    def validate_config(self) -> None:
        """Validate that upsert is supported.

        Raises:
            ValueError: If upsert requirements are not met.
        """
        # Check if custom merge_upsert_from_table is implemented
        has_custom_merge = (
            type(self.sink).merge_upsert_from_table
            != self.sink.__class__.__bases__[  # type: ignore[attr-defined]
                0
            ].merge_upsert_from_table
        )

        # If custom merge exists, check allow_merge_upsert capability
        if has_custom_merge:
            if not self.connector.allow_merge_upsert:
                msg = (
                    f"UPSERT load method requires allow_merge_upsert = True "
                    f"in {self.connector.__class__.__name__}"
                )
                raise ValueError(msg)
            return

        # For generic upsert, check temp table support
        if not self.connector.allow_temp_tables:
            msg = (
                f"UPSERT load method requires temp table support. "
                f"Either set allow_temp_tables = True in {self.connector.__class__.__name__} "  # noqa: E501
                f"or implement merge_upsert_from_table() with allow_merge_upsert = True"
            )
            raise ValueError(msg)

        # Check that primary keys are defined
        if not self.sink.key_properties:
            msg = (
                "UPSERT load method requires primary keys to be defined. "
                "Set key_properties in your stream or use append-only mode."
            )
            raise ValueError(msg)

    def prepare_table(
        self,
        full_table_name: str,
        schema: dict,
        primary_keys: t.Sequence[str],
    ) -> None:
        """Prepare table for upsert loading.

        Ensures table exists with correct schema and primary keys.

        Args:
            full_table_name: The fully qualified table name.
            schema: The JSON schema for the table.
            primary_keys: List of primary key column names.
        """
        # Check if table exists
        if not self.connector.table_exists(full_table_name):
            # Create new table
            self.logger.info("Creating table %s for UPSERT", full_table_name)
            self.connector.create_empty_table(
                full_table_name=full_table_name,
                schema=schema,
                primary_keys=primary_keys,
                as_temp_table=False,
            )
            return

        # Table exists - add any new columns
        for property_name, property_def in schema["properties"].items():
            self.connector.prepare_column(
                full_table_name,
                property_name,
                self.connector.to_sql_type(property_def),
            )

        # Ensure primary keys are set (important for upsert)
        self.connector.prepare_primary_key(
            full_table_name=full_table_name,
            primary_keys=primary_keys,
        )
