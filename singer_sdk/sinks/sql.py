"""Sink classes load data to SQL targets."""

from typing import Any, Dict, Iterable, List, Optional, Type

import sqlalchemy
from pendulum import now

from singer_sdk.plugin_base import PluginBase
from singer_sdk.sinks.batch import BatchSink
from singer_sdk.streams.sql import SQLConnector


class SQLSink(BatchSink):
    """SQL-type sink type."""

    connector_class: Type[SQLConnector]
    soft_delete_column_name = "_sdc_deleted_at"
    version_column_name = "_sdc_table_version"

    def __init__(
        self,
        target: PluginBase,
        stream_name: str,
        schema: Dict,
        key_properties: Optional[List[str]],
        connector: Optional[SQLConnector] = None,
    ) -> None:
        """Initialize SQL Sink.

        Args:
            target: The target object.
            stream_name: The source tap's stream name.
            schema: The JSON Schema definition.
            key_properties: The primary key columns.
            connector: Optional connector to reuse.
        """
        self._connector: SQLConnector
        if connector:
            self._connector = connector
        else:
            self._connector = self.connector_class(dict(target.config))

        super().__init__(target, stream_name, schema, key_properties)

    @property
    def connector(self) -> SQLConnector:
        """The connector object.

        Returns:
            The connector object.
        """
        return self._connector

    @property
    def connection(self) -> sqlalchemy.engine.Connection:
        """Get or set the SQLAlchemy connection for this sink.

        Returns:
            A connection object.
        """
        return self.connector.connection

    @property
    def table_name(self) -> str:
        """Returns the table name, with no schema or database part.

        Returns:
            The target table name.
        """
        parts = self.stream_name.split("-")

        if len(parts) == 1:
            return self.stream_name
        else:
            return parts[-1]

    @property
    def schema_name(self) -> Optional[str]:
        """Returns the schema name or `None` if using names with no schema part.

        Returns:
            The target schema name.
        """
        return None  # Assumes single-schema target context.

    @property
    def database_name(self) -> Optional[str]:
        """Returns the DB name or `None` if using names with no database part.

        Returns:
            The target database name.
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
        self.connector.prepare_table(
            full_table_name=self.full_table_name,
            schema=self.schema,
            primary_keys=self.key_properties,
            as_temp_table=False,
        )
        self.bulk_insert_records(
            full_table_name=self.full_table_name,
            schema=self.schema,
            records=context["records"],
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

    def create_table_with_records(
        self,
        full_table_name: Optional[str],
        schema: dict,
        records: Iterable[Dict[str, Any]],
        primary_keys: Optional[List[str]] = None,
        partition_keys: Optional[List[str]] = None,
        as_temp_table: bool = False,
    ) -> None:
        """Create an empty table.

        Args:
            full_table_name: the target table name.
            schema: the JSON schema for the new table.
            records: records to load.
            primary_keys: list of key properties.
            partition_keys: list of partition keys.
            as_temp_table: True to create a temp table.
        """
        full_table_name = full_table_name or self.full_table_name
        if primary_keys is None:
            primary_keys = self.key_properties
        partition_keys = partition_keys or None
        self.connector.prepare_table(
            full_table_name=full_table_name,
            primary_keys=primary_keys,
            schema=schema,
            as_temp_table=as_temp_table,
        )
        self.bulk_insert_records(
            full_table_name=full_table_name, schema=schema, records=records
        )

    def bulk_insert_records(
        self,
        full_table_name: str,
        schema: dict,
        records: Iterable[Dict[str, Any]],
    ) -> Optional[int]:
        """Bulk insert records to an existing destination table.

        The default implementation uses a generic SQLAlchemy bulk insert operation.
        This method may optionally be overridden by developers in order to provide
        faster, native bulk uploads.

        Args:
            full_table_name: the target table name.
            schema: the JSON schema for the new table, to be used when inferring column
                names.
            records: the input records.

        Returns:
            True if table exists, False if not, None if unsure or undetectable.
        """
        property_names = list(schema["properties"].keys())
        insert_sql = sqlalchemy.text(
            f"INSERT INTO {full_table_name} VALUES "
            f"({', '.join([':' + n for n in property_names])})"
        )
        self.connector.connection.execute(
            insert_sql,
            records,
        )
        if isinstance(records, list):
            return len(records)  # If list, we can quickly return record count.

        return None  # Unknown record count.

    def merge_upsert_from_table(
        self, target_table_name: str, from_table_name: str, join_keys: List[str]
    ) -> Optional[int]:
        """Merge upsert data from one table to another.

        Args:
            target_table_name: The destination table name.
            from_table_name: The source table name.
            join_keys: The merge upsert keys, or `None` to append.

        Return:
            The number of records copied, if detectable, or `None` if the API does not
            report number of records affected/inserted.

        Raises:
            NotImplementedError: if the merge upsert capability does not exist or is
                undefined.
        """
        raise NotImplementedError()

    def activate_version(self, new_version: int) -> None:
        """Bump the active version of the target table.

        Args:
            new_version: The version number to activate.
        """
        deleted_at = now()

        if self.config.get("hard_delete", True):
            self.connection.execute(
                f"DELETE FROM {self.full_table_name} "
                f"WHERE {self.version_column_name} <= {new_version}"
            )
            return

        if not self.connector.column_exists(
            full_table_name=self.full_table_name,
            column_name=self.soft_delete_column_name,
        ):
            self.connector.prepare_column(
                self.full_table_name,
                self.soft_delete_column_name,
                sql_type=sqlalchemy.types.DateTime(),
            )
        self.connection.execute(
            f"UPDATE {self.full_table_name}\n"
            f"SET {self.soft_delete_column_name} = ? \n"
            f"WHERE {self.version_column_name} < ? \n"
            f"  AND {self.soft_delete_column_name} IS NULL\n",
            deleted_at,
            new_version,
        )


__all__ = ["SQLSink", "SQLConnector"]
