"""{{ cookiecutter.destination_name }} target sink class, which handles writing streams."""

from __future__ import annotations

import typing as t

{%- set
    sinkclass_mapping = {
        "Per batch": "BatchSink",
        "Per record": "RecordSink",
        "SQL": "SQLSink",
    }
%}

{%- set sinkclass = sinkclass_mapping[cookiecutter.serialization_method] %}

{%- if sinkclass == "SQLSink" %}

import sqlalchemy
from singer_sdk.connectors import SQLConnector
from singer_sdk.sinks import {{ sinkclass }}
{%- else %}

from singer_sdk.sinks import {{ sinkclass }}
{%- endif %}


{%- if sinkclass == "SQLSink" %}


class {{ cookiecutter.destination_name }}Connector(SQLConnector):
    """The connector for {{ cookiecutter.destination_name }}.

    This class handles all DDL and type conversions.
    """

    allow_column_add: bool = True  # Whether ADD COLUMN is supported.
    allow_column_rename: bool = True  # Whether RENAME COLUMN is supported.
    allow_column_alter: bool = False  # Whether altering column types is supported.
    allow_merge_upsert: bool = False  # Whether MERGE UPSERT is supported.
    allow_overwrite: bool = False  # Whether overwrite load method is supported.
    allow_temp_tables: bool = True  # Whether temp tables are supported.

    def get_sqlalchemy_url(self, config: dict) -> str:
        """Generates a SQLAlchemy URL for {{ cookiecutter.destination_name }}.

        Args:
            config: The configuration for the connector.
        """
        return super().get_sqlalchemy_url(config)
{%- endif %}


class {{ cookiecutter.destination_name }}Sink({{ sinkclass }}):
    """{{ cookiecutter.destination_name }} target sink class."""

    {% if sinkclass == "RecordSink" -%}
    def process_record(self, record: dict, context: dict) -> None:
        """Process the record.

        Args:
            record: Individual record in the stream.
            context: Stream partition or context dictionary.
        """
        # Sample:
        # ------
        # client.write(record)  # noqa: ERA001

    {%- elif sinkclass == "BatchSink" -%}

    max_size = 10000  # Max records to write in one batch

    def start_batch(self, context: dict) -> None:
        """Start a batch.

        Developers may optionally add additional markers to the `context` dict,
        which is unique to this batch.

        Args:
            context: Stream partition or context dictionary.
        """
        # Sample:
        # ------
        # batch_key = context["batch_id"]
        # context["file_path"] = f"{batch_key}.csv"

    def process_record(self, record: dict, context: dict) -> None:
        """Process the record.

        Developers may optionally read or write additional markers within the
        passed `context` dict from the current batch.

        Args:
            record: Individual record in the stream.
            context: Stream partition or context dictionary.
        """
        # Sample:
        # ------
        # with open(context["file_path"], "a") as csvfile:
        #     csvfile.write(record)

    def process_batch(self, context: dict) -> None:
        """Write out any prepped records and return once fully written.

        Args:
            context: Stream partition or context dictionary.
        """
        # Sample:
        # ------
        # client.upload(context["file_path"])  # Upload file
        # Path(context["file_path"]).unlink()  # Delete local copy

    {%- elif sinkclass == "SQLSink" -%}

    connector_class = {{ cookiecutter.destination_name }}Connector

    def setup(self) -> None:
        """Set up Sink.

        This method is called on Sink creation, and creates the required Schema and
        Table entities in the target database. Developers may override this method
        to customize schema and table preparation logic.
        """
        # Prepare schema if specified
        if self.schema_name:
            self.connector.prepare_schema(self.schema_name)

        # Prepare table with schema validation
        with self.connector._connect() as connection, connection.begin():
            self.connector.prepare_table(
                full_table_name=self.full_table_name,
                schema=self.schema,
                primary_keys=self.key_properties,
                connection=connection,
                as_temp_table=False,
            )

    def process_batch(self, context: dict) -> None:
        """Process a batch with the given batch context.

        Writes a batch to the SQL target. Developers may override this method
        in order to provide a more efficient upload/upsert process.

        The default implementation uses a temporary table approach for optimal
        performance with large datasets and proper transaction handling.

        Args:
            context: Stream partition or context dictionary.
        """
        # Use one connection so we do this all in a single transaction
        with self.connector._connect() as connection, connection.begin():
            # Check structure of target table
            table = self.connector.prepare_table(
                full_table_name=self.full_table_name,
                schema=self.schema,
                primary_keys=self.key_properties,
                as_temp_table=False,
                connection=connection,
            )

            # Create a temporary table for staging records
            temp_table_name = self.generate_temp_table_name()
            temp_table = self.connector.copy_table_structure(
                full_table_name=temp_table_name,
                from_table=table,
                as_temp_table=True,
                connection=connection,
            )

            try:
                # Insert records into temporary table
                self.bulk_insert_records(
                    table=temp_table,
                    schema=self.schema,
                    primary_keys=self.key_properties,
                    records=context["records"],
                    connection=connection,
                )

                # Merge data from temporary table to target table
                self.upsert(
                    from_table=temp_table,
                    to_table=table,
                    schema=self.schema,
                    join_keys=self.key_properties,
                    connection=connection,
                )
            finally:
                # Always clean up temporary table
                self.connector.drop_table(table=temp_table, connection=connection)

    def generate_temp_table_name(self) -> str:
        """Generate a unique temporary table name.

        Developers may override this method to customize temporary table naming.
        Default implementation creates a UUID-based name to avoid collisions.

        Returns:
            A unique temporary table name.
        """
        import uuid

        return f"temp_{str(uuid.uuid4()).replace('-', '_')}"

    def bulk_insert_records(
        self,
        table: sqlalchemy.Table,
        schema: dict,
        records: t.Iterable[dict[str, t.Any]],
        primary_keys: t.Sequence[str],
        connection: sqlalchemy.engine.Connection,
    ) -> int | None:
        """Bulk insert records to an existing destination table.

        Developers may override this method to provide faster, native bulk uploads
        specific to their target database. The default implementation uses
        SQLAlchemy's bulk insert operations.

        Args:
            table: The target table object.
            schema: The JSON schema for the new table, to be used when inferring column
                names.
            records: The input records.
            primary_keys: The primary key columns for the table.
            connection: The database connection.

        Returns:
            Number of records inserted, or None if not detectable.
        """
        import sqlalchemy as sa

        columns = self.column_representation(schema)
        data = []

        # Determine if we need to handle duplicates
        append_only = not primary_keys
        unique_records = {}

        for record in records:
            insert_record = {column.name: record.get(column.name) for column in columns}

            if append_only:
                data.append(insert_record)
            else:
                # Keep only the latest record per primary key
                primary_key_tuple = tuple(record[key] for key in primary_keys)
                unique_records[primary_key_tuple] = insert_record

        if not append_only:
            data = list(unique_records.values())

        if data:
            insert_stmt = sa.insert(table)
            connection.execute(insert_stmt, data)
            return len(data)

        return 0

    def upsert(
        self,
        from_table: sqlalchemy.Table,
        to_table: sqlalchemy.Table,
        schema: dict,
        join_keys: t.Sequence[str],
        connection: sqlalchemy.engine.Connection,
    ) -> int | None:
        """Merge upsert data from one table to another.

        Developers may override this method to provide database-specific
        MERGE or UPSERT operations for better performance. The default
        implementation uses separate INSERT and UPDATE operations.

        Args:
            from_table: The source table.
            to_table: The destination table.
            schema: Singer Schema message.
            join_keys: The merge upsert keys, or `None` to append.
            connection: The database connection.

        Returns:
            The number of records copied, if detectable, or `None` if the API does not
            report number of records affected/inserted.
        """
        import sqlalchemy as sa

        if not join_keys:
            # Append-only mode: simple insert
            select_stmt = sa.select(from_table.columns).select_from(from_table)
            insert_stmt = to_table.insert().from_select(
                names=from_table.columns, select=select_stmt
            )
            connection.execute(insert_stmt)
        else:
            # Upsert mode: insert new records, update existing ones

            # 1. Insert new records (those not matching existing primary keys)
            join_predicates = []
            for key in join_keys:
                from_table_key = from_table.columns[key]
                to_table_key = to_table.columns[key]
                join_predicates.append(from_table_key == to_table_key)

            join_condition = sa.and_(*join_predicates)

            where_predicates = []
            for key in join_keys:
                to_table_key = to_table.columns[key]
                where_predicates.append(to_table_key.is_(None))
            where_condition = sa.and_(*where_predicates)

            select_stmt = (
                sa.select(from_table.columns)
                .select_from(from_table.outerjoin(to_table, join_condition))
                .where(where_condition)
            )
            insert_stmt = sa.insert(to_table).from_select(
                names=from_table.columns, select=select_stmt
            )
            connection.execute(insert_stmt)

            # 2. Update existing records
            update_columns = {}
            for column_name in schema["properties"]:
                from_table_column = from_table.columns[column_name]
                to_table_column = to_table.columns[column_name]
                update_columns[to_table_column] = from_table_column

            update_stmt = (
                sa.update(to_table).where(join_condition).values(update_columns)
            )
            connection.execute(update_stmt)

        return None

    def column_representation(
        self,
        schema: dict,
    ) -> list[sqlalchemy.Column]:
        """Return a sqlalchemy table representation for the current schema.

        Developers should not need to override this method unless they need
        custom column definitions or type mappings.

        Args:
            schema: The JSON schema for the table.

        Returns:
            List of SQLAlchemy Column objects.
        """
        import sqlalchemy as sa

        columns = [
            sa.Column(
                property_name,
                self.connector.to_sql_type(property_jsonschema),
            )
            for property_name, property_jsonschema in schema["properties"].items()
        ]
        return columns

    @property
    def schema_name(self) -> str | None:
        """Return the schema name or `None` if using names with no schema part.

        Developers may override this method to customize schema name resolution
        based on configuration or stream naming conventions.

        Returns:
            The target schema name.
        """
        # Look for a default_target_schema in the configuration file
        default_target_schema = self.config.get("default_target_schema")
        if default_target_schema:
            return default_target_schema

        # Parse schema from stream name if in <schema>-<table> format
        parts = self.stream_name.split("-")
        if len(parts) in {2, 3}:
            return parts[-2]

        return None
    {%- endif %}