"""Sink classes load data to SQL targets."""

from enum import Enum
from typing import Any, Dict, Iterable, List, Optional
from numpy import full
from pendulum import now

import sqlalchemy
from singer_sdk.plugin_base import PluginBase
from singer_sdk.sinks.batch import BatchSink
from singer_sdk.streams.sql import SQLConnector
from singer_sdk import typing as th


class SQLSink(BatchSink):
    """SQL-type sink type."""

    connector: SQLConnector
    soft_delete_column_name = "_sdc_deleted_at"
    version_column_name = "_sdc_table_version"

    class TableType(Enum):
        """Controls the available table types for a connector."""

        PERMANENT: str = "PERMANENT"
        TEMPORARY: str = "TEMPORARY"

    def __init__(
        self,
        target: PluginBase,
        stream_name: str,
        schema: Dict,
        key_properties: Optional[List[str]],
    ) -> None:
        """Initialize SQL Sink.

        Args:
            target: The target object.
            stream_name: The source tap's stream name.
            schema: The JSON Schema definition.
            key_properties: The primary key columns.
        """
        super().__init__(target, stream_name, schema, key_properties)

    @property
    def connection(self) -> sqlalchemy.engine.Connection:
        """Get or set the SQLAlchemy connection for this sink.

        Returns:
            A connection object.
        """
        return self.connector.connection

    @property
    def table_name(self) -> str:
        """Returns the table name, with no schema or database part."""
        pass

    @property
    def schema_name(self) -> Optional[str]:
        """Returns the schema name or `None` if using names with no schema part.

        Return:
            The schema name.
        """
        pass

    @property
    def database_name(self) -> Optional[str]:
        """Returns the DB name or `None` if using names with no database part.

        Returns:
            The database name.
        """
        return None  # Assumes single-DB target context.

    def process_batch(self, context: dict) -> None:
        """Process a batch with the given batch context.

        Writes a batch to the SQL target. Developers may override this method
        in order to provide a more efficient upload/upsert process.

        Args:
            context: Stream partition or context dictionary.
        """
        # If duplicates are merged, these can be tracked via
        # :meth:`~singer_sdk.Sink.tally_duplicate_merged()`.

        if not self.target_table_exists:
            self.create_table_from_records(context["records"])
            return

        self.prepare_target_table(
            full_table_name=self.full_table_name,
            schema=self.schema,
            primary_keys=self.key_properties,
        )

    @property
    def full_table_name(self) -> str:
        """Gives the fully qualified table name.

        Returns:
            The fully qualified table name.
        """
        return self.connector.get_fully_qualified_name(
            self.table_name,
            self.schema_name,
            self.database_name,
        )

    def target_table_exists(
        self, full_table_name: Optional[str] = None
    ) -> Optional[bool]:
        """Determine if the target table already exists.

        Args:
            full_table_name: the target table name.

        Returns:
            True if table exists, False if not, None if unsure or undetectable.
        """
        return None

    def target_column_exists(
        self,
        full_table_name: Optional[str] = None,
        column_name: Optional[str] = None,
    ) -> Optional[bool]:
        """Determine if the target table already exists.

        Args:
            full_table_name: the target table name.

        Returns:
            True if table exists, False if not, None if unsure or undetectable.
        """
        return None

    def create_table_with_records(
        self,
        full_table_name: Optional[str],
        schema: dict,
        records: Iterable[Dict[str, Any]],
        primary_keys: Optional[List[str]] = None,
        partition_keys: Optional[List[str]] = None,
        table_type: TableType = TableType.PERMANENT,
    ) -> None:
        """Create an empty table.

        Args:
            full_table_name: the target table name.
            schema: the JSON schema for the new table.
            records: records to load.
            primary_keys: list of key properties.
            partition_keys: list of partition keys.
            table_type: permanent, temporary, or a proprietary type.
        """
        full_table_name = full_table_name or self.full_table_name
        if primary_keys is None:
            primary_keys = self.key_properties
        partition_keys = partition_keys or None
        self.prepare_target_table(
            full_table_name=full_table_name,
            primary_keys=primary_keys,
            schema=schema,
            table_type=table_type,
        )
        self.bulk_insert_records(full_table_name=full_table_name, records=records)

    def bulk_insert_records(
        self, full_table_name: str, records: Iterable[Dict[str, Any]]
    ) -> Optional[int]:
        """Bulk insert records to an existing destination table.

        Args:
            full_table_name: the target table name.
            records: the input records.

        Return:
            True if table exists, False if not, None if unsure or undetectable.
        """
        pass

    def merge_upsert_from_table(
        self, target_table_name: str, from_table_name: str, join_keys: List[str]
    ) -> Optional[int]:
        """Merge upsert data from one table to another.

        Args:
            target_table_name (str): The destination table name.
            from_table_name (str): The source table name.
            join_keys (List[str]): The merge upsert keys, or `None` to append.

        Return:
            The number of records copied, if detectable, or `None` if the API does not
            report number of records affected/inserted.
        """
        pass

    def prepare_target_table(
        self,
        full_table_name: str,
        schema: dict,
        primary_keys: List[str],
        partition_keys: Optional[List[str]] = None,
        table_type: TableType = TableType.PERMANENT,
    ) -> None:
        """Adapt target table to provided schema if possible.

        Args:
            full_table_name: the target table name.
            schema: the import schema.
            primary_keys: list of key properties.
            partition_keys: list of partition keys.
            table_type: permanent, temporary, or a proprietary type.
        """
        if not self.target_table_exists(full_table_name=full_table_name):
            self.create_empty_table(
                full_table_name=full_table_name,
                schema=schema,
                primary_keys=primary_keys,
                partition_keys=partition_keys,
                table_type=table_type,
            )
            return

        for property_name, property_def in schema["properties"].items():
            self.prepare_target_column(full_table_name, property_name, property_def)

    def prepare_target_column(
        self,
        full_table_name: str,
        column_name: str,
        schema: dict,
    ) -> None:
        """Adapt target table to provided schema if possible.

        Args:
            full_table_name: the target table name.
            column_name: the target column name.
            schema: the import schema.
        """
        if not self.target_column_exists(full_table_name, column_name):
            self.create_column(
                full_table_name=full_table_name,
                column_name=column_name,
                schema=schema,
            )
            return

        self.adapt_column_to_type(
            full_table_name,
            column_name=column_name,
            schema=schema,
        )

    def create_empty_table(
        self,
        full_table_name: str,
        schema: dict,
        primary_keys: Optional[List[str]] = None,
        partition_keys: Optional[List[str]] = None,
        table_type: TableType = TableType.PERMANENT,
    ) -> None:
        """Create an empty target table.

        Args:
            full_table_name: the target table name.
            schema: the JSON schema for the new table.
            primary_keys: list of key properties.
            partition_keys: list of partition keys.
            table_type: permanent, temporary, or a proprietary type.
        """
        pass

    def create_empty_column(
        self,
        full_table_name: str,
        column_name: str,
        schema: dict,
    ):
        """Create a new column.

        Args:
            full_table_name: The target table name.
            column_name: The name of the new column.
            schema: JSON Schema type definition to be used in creating the new column.
        """

    def activate_table_version(
        self,
        full_table_name: str,
        table_version: int,
    ):
        if self.config.get("hard_delete", True):
            self.connection.execute(
                f"DELETE FROM {self.full_table_name} "
                f"WHERE {self.version_column_name} <= {table_version}"
            )
        if not self.target_column_exists(
            full_table_name=full_table_name, column_name=self.soft_delete_column_name
        ):
            self.create_target_column(
                self.full_table_name, self.soft_delete_column_name, th.DateTimeType()
            )
        self.connection.execute(
            f"UPDATE {self.full_table_name} "
            f"SET {self.soft_delete_column_name} = '{now().isoformat()}'"
        )

    def adapt_column_to_type(
        self, full_table_name: str, column_name: str, schema: dict
    ):
        current_sql_type = _
        needed_sql_type = _
        compatible_sql_type = self.get_merged_sql_type(
            current_sql_type, needed_sql_type
        )
        self.connection.execute(
            f"ALTER TABLE {full_table_name} "
            f"ALTER COLUMN {column_name} ({compatible_sql_type})"
        )
