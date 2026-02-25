"""SQL client handling.

This includes {{ cookiecutter.source_name }}Stream and {{ cookiecutter.source_name }}Connector.
"""

from __future__ import annotations

import functools
import sys
from typing import TYPE_CHECKING, Any

from singer_sdk import SQLConnector, SQLStream
from singer_sdk.connectors.sql import SQLToJSONSchema
from singer_sdk.helpers._typing import TypeConformanceLevel

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    from collections.abc import Iterable

    import sqlalchemy
    from singer_sdk.helpers.types import Context, Record
    from sqlalchemy.engine import Engine
    from sqlalchemy.engine.reflection import Inspector


class {{ cookiecutter.source_name }}SQLToJSONSchema(SQLToJSONSchema):
    """Custom SQL to JSON Schema conversion for {{ cookiecutter.source_name }}.

    Developers should override this class to customize how SQL types are converted
    to JSON Schema types. This is particularly useful for databases with custom
    or non-standard SQL types.
    """

    def __init__(
        self,
        *,
        custom_config_option: bool = False,
        **kwargs: Any,
    ) -> None:
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
    def to_jsonschema(self, column_type: Any) -> dict:
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
            sql_type: The SQL type as a string or as a TypeEngine. If a TypeEngine is
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

        return query  # noqa: RET504

    @override
    def get_records(self, context: Context | None) -> Iterable[Record]:
        """Return a generator of record-type dictionary objects.

        Developers may override this method to implement custom record retrieval
        logic, such as batch processing, result set optimization, or custom
        data transformations. This is particularly useful when the source database
        provides batch-optimized record retrieval mechanisms.

        Args:
            context: If provided, will read specifically from this data slice.

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

            start_val = self.get_starting_replication_key_value(context)
            if start_val:
                query = query.where(replication_key_col >= start_val)

        # 3. Execute query and yield records
        with self.connector._connect() as connection:  # noqa: SLF001
            for record in connection.execute(query).mappings():
                yield dict(record)

        # Alternative: Use the default implementation
        # yield from super().get_records(context)
