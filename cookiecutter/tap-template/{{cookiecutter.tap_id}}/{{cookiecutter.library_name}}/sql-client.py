"""SQL client handling.

This includes {{ cookiecutter.source_name }}Stream and {{ cookiecutter.source_name }}Connector.
"""

from __future__ import annotations

import functools
import sys
import typing as t

import sqlalchemy
from singer_sdk import SQLConnector, SQLStream
from singer_sdk.connectors.sql import SQLToJSONSchema
from singer_sdk.helpers._typing import TypeConformanceLevel

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context
    from sqlalchemy.engine import Engine
    from sqlalchemy.engine.reflection import Inspector


class {{ cookiecutter.source_name }}SQLToJSONSchema(SQLToJSONSchema):
    """Custom SQL to JSON Schema conversion for {{ cookiecutter.source_name }}.

    Developers should override this class to customize how SQL types are converted
    to JSON Schema types. This is particularly useful for databases with custom
    or non-standard SQL types.
    """

    def __init__(self, *, custom_config_option: bool = False, **kwargs):
        """Initialize the SQL to JSON Schema converter.

        Args:
            custom_config_option: Example custom configuration option.
            **kwargs: Additional keyword arguments passed to parent class.
        """
        super().__init__(**kwargs)
        self.custom_config_option = custom_config_option

    @override
    @classmethod
    def from_config(cls, config: dict) -> {{ cookiecutter.source_name }}SQLToJSONSchema:
        """Instantiate the SQL to JSON Schema converter from a config dictionary.

        Developers should override this method to pass custom configuration options
        from the tap's config to the converter.

        Args:
            config: The tap's configuration dictionary.

        Returns:
            An instance of the SQL to JSON Schema converter.
        """
        return cls(
            custom_config_option=config.get("custom_config_option", False),
        )

    @override
    @functools.singledispatchmethod
    def to_jsonschema(self, column_type: t.Any) -> dict:
        """Customize the JSON Schema for {{ cookiecutter.source_name }} types.

        Developers should not need to override this base method. Instead, register
        specific type handlers using the @to_jsonschema.register decorator for
        specific SQLAlchemy column types.

        Args:
            column_type: The SQLAlchemy column type to convert.

        Returns:
            A JSON Schema type definition.
        """
        return super().to_jsonschema(column_type)

    # Example: Custom type conversion for database-specific types
    # @to_jsonschema.register
    # def custom_type_to_jsonschema(self, column_type: CustomSQLType) -> dict:
    #     """Override the default mapping for CustomSQLType columns.
    #
    #     Developers may add custom type mappings by registering handlers
    #     for specific SQLAlchemy column types.
    #     """
    #     if self.custom_config_option:
    #         return {"type": ["string", "null"]}
    #     return {"type": ["object", "null"], "additionalProperties": True}


class {{ cookiecutter.source_name }}Connector(SQLConnector):
    """Connects to the {{ cookiecutter.source_name }} SQL source.

    This class handles all SQL connection logic and DDL operations.
    Developers may override methods to customize connection behavior,
    schema discovery, and type conversion.
    """

    # Custom SQL to JSON Schema converter
    sql_to_jsonschema_converter = {{ cookiecutter.source_name }}SQLToJSONSchema

    @override
    def get_sqlalchemy_url(self, config: dict) -> str:
        """Concatenate a SQLAlchemy URL for use in connecting to the source.

        Developers must implement this method to return a valid SQLAlchemy
        connection string for your specific database.

        Args:
            config: A dict with connection parameters

        Returns:
            SQLAlchemy connection string
        """
        # TODO: Replace this with a valid connection string for your source:
        return (
            f"awsathena+rest://{config['aws_access_key_id']}:"
            f"{config['aws_secret_access_key']}@athena"
            f".{config['aws_region']}.amazonaws.com:443/"
            f"{config['schema_name']}?"
            f"s3_staging_dir={config['s3_staging_dir']}"
        )

    @override
    def get_schema_names(self, engine: Engine, inspected: Inspector) -> list[str]:
        """Return a list of schema names in DB, or overrides with user-provided values.

        Developers may override this method to customize schema discovery,
        such as filtering out system schemas or applying user-defined filters.

        Args:
            engine: SQLAlchemy engine
            inspected: SQLAlchemy inspector instance for engine

        Returns:
            List of schema names
        """
        return super().get_schema_names(engine, inspected)

    @override
    def to_jsonschema_type(
        self,
        sql_type: str | sqlalchemy.types.TypeEngine | type[sqlalchemy.types.TypeEngine],
    ) -> dict:
        """Returns a JSON Schema equivalent for the given SQL type.

        Developers may optionally add custom logic before calling the default
        implementation inherited from the base class. For more complex type
        conversion needs, consider overriding the sql_to_jsonschema_converter class.

        Args:
            from_type: The SQL type as a string or as a TypeEngine. If a TypeEngine is
                provided, it may be provided as a class or a specific object instance.

        Returns:
            A compatible JSON Schema type definition.
        """
        # Optionally, add custom logic before calling the parent SQLConnector method.
        # You may delete this method if overrides are not needed.
        return super().to_jsonschema_type(sql_type)

    @override
    def to_sql_type(self, jsonschema_type: dict) -> sqlalchemy.types.TypeEngine:
        """Returns a JSON Schema equivalent for the given SQL type.

        Developers may optionally add custom logic before calling the default
        implementation inherited from the base class.

        Args:
            jsonschema_type: A dict

        Returns:
            SQLAlchemy type
        """
        # Optionally, add custom logic before calling the parent SQLConnector method.
        # You may delete this method if overrides are not needed.
        return super().to_sql_type(jsonschema_type)

    # Uncomment and customize these methods as needed for your specific database:

    # @override
    # def get_object_names(
    #     self, engine: Engine, inspected: Inspector, schema_name: str
    # ) -> list[tuple[str, bool]]:
    #     """Return a list of syncable objects in the given schema.
    #
    #     Developers may override this method to customize which database objects
    #     (tables, views) are discoverable by the tap.
    #
    #     Args:
    #         engine: SQLAlchemy engine
    #         inspected: SQLAlchemy inspector instance
    #         schema_name: Schema name to inspect
    #
    #     Returns:
    #         List of tuples: (object_name, is_view)
    #     """
    #     return super().get_object_names(engine, inspected, schema_name)

    # @override
    # def prepare_column(
    #     self, full_table_name: str, column_name: str, sql_type: sqlalchemy.types.TypeEngine
    # ) -> sqlalchemy.Column:
    #     """Prepare a column object for table creation or modification.
    #
    #     Developers may override this method to customize column definitions,
    #     such as adding database-specific constraints or modifiers.
    #     """
    #     return super().prepare_column(full_table_name, column_name, sql_type)

    # @override
    # def create_engine(self) -> sqlalchemy.engine.Engine:
    #     """Create a SQLAlchemy engine for database connections.
    #
    #     Developers may override this method to customize engine creation,
    #     such as adding connection pooling, custom dialects, or connection arguments.
    #     """
    #     return super().create_engine()

    # @override
    # def open_connection(self) -> sqlalchemy.engine.Connection:
    #     """Open a new database connection.
    #
    #     Developers may override this method to customize connection behavior,
    #     such as setting session variables or connection-specific configurations.
    #     """
    #     return super().open_connection()


class {{ cookiecutter.source_name }}Stream(SQLStream):
    """Stream class for {{ cookiecutter.source_name }} streams.

    This class handles stream-specific logic including record retrieval,
    query filtering, and data processing. Developers may override methods
    to customize stream behavior.
    """

    connector_class = {{ cookiecutter.source_name }}Connector

    # Query and data processing configuration
    supports_nulls_first = True  # Whether the database supports NULLS FIRST/LAST

    # Type conformance level - controls how strictly data types are enforced
    # ROOT_ONLY: Only enforce types at the root level (useful for JSON/JSONB columns)
    # RECURSIVE: Recursively enforce types throughout nested structures
    TYPE_CONFORMANCE_LEVEL = TypeConformanceLevel.RECURSIVE

    @override
    def apply_query_filters(
        self,
        query: sqlalchemy.sql.Select,
        table: sqlalchemy.Table,
        *,
        context: Context | None = None,
    ) -> sqlalchemy.sql.Select:
        """Apply custom query filters to the SELECT query.

        Developers may override this method to add custom WHERE clauses,
        JOIN operations, or other query modifications based on configuration
        or stream-specific requirements.

        Args:
            query: The base SELECT query
            table: The SQLAlchemy table object
            context: Stream partition or context dictionary

        Returns:
            Modified SELECT query
        """
        query = super().apply_query_filters(query, table, context=context)

        # Add custom WHERE clauses from configuration, etc.
        # query = query.where(...)

        return query

    @override
    def get_records(self, partition: Context | None) -> t.Iterable[dict[str, t.Any]]:
        """Return a generator of record-type dictionary objects.

        Developers may override this method to implement custom record retrieval
        logic, such as batch processing, result set optimization, or custom
        data transformations. This is particularly useful when the source database
        provides batch-optimized record retrieval mechanisms.

        Args:
            partition: If provided, will read specifically from this data slice.

        Yields:
            One dict per record.
        """
        # Example implementation with query optimization:
        # 1. Get selected columns to avoid SELECT *
        selected_column_names = self.get_selected_schema()["properties"].keys()
        table = self.connector.get_table(
            full_table_name=self.fully_qualified_name,
            column_names=selected_column_names,
        )
        query = table.select()

        # 2. Apply replication key ordering and filtering
        if self.replication_key:
            replication_key_col = table.columns[self.replication_key]
            query = query.order_by(replication_key_col)

            start_val = self.get_starting_replication_key_value(partition)
            if start_val:
                query = query.where(replication_key_col >= start_val)

        # 3. Execute query and yield records
        with self.connector._connect() as connection:
            for record in connection.execute(query).mappings():
                yield dict(record)

        # Alternative: Use the default implementation
        # yield from super().get_records(partition)

    # Uncomment and customize these methods as needed for your specific database:

    # @override
    # def get_starting_replication_key_value(
    #     self, context: Context | None
    # ) -> t.Any | None:
    #     """Get starting replication key value for incremental sync.
    #
    #     Developers may override this method to customize how the starting
    #     replication key value is determined, such as converting timestamps
    #     to native database types.
    #     """
    #     return super().get_starting_replication_key_value(context)

    # @override
    # def post_process(self, row: dict, context: Context | None = None) -> dict | None:
    #     """Process a single record after it's been retrieved from the database.
    #
    #     Developers may override this method to apply custom transformations,
    #     data validation, or filtering to individual records before they are
    #     emitted to the output stream.
    #
    #     Args:
    #         row: Individual record dictionary
    #         context: Stream partition or context dictionary
    #
    #     Returns:
    #         Processed record dictionary, or None to skip the record
    #     """
    #     return row

    # @override
    # def validate_config(self) -> None:
    #     """Validate tap configuration.
    #
    #     Developers may override this method to add custom configuration
    #     validation logic specific to their database requirements.
    #     """
    #     super().validate_config()
    #
    #     # Example: Validate required custom configuration
    #     # if not self.config.get("custom_required_field"):
    #     #     raise ValueError("custom_required_field is required")

    # @override
    # @property
    # def is_sorted(self) -> bool:
    #     """Indicate whether the stream is sorted by replication key.
    #
    #     Developers may override this property to indicate whether their
    #     query results are naturally sorted by the replication key, which
    #     can enable more efficient incremental sync processing.
    #     """
    #     return bool(self.replication_key)

    # def _increment_stream_state(
    #     self, latest_record: dict, *, context: Context | None = None
    # ) -> None:
    #     """Update the stream state with the latest record's replication key value.
    #
    #     Developers typically should not need to override this method unless
    #     implementing custom state management logic (e.g., for log-based replication).
    #     """
    #     return super()._increment_stream_state(latest_record, context=context)
