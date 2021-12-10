"""Base class for SQL-type streams."""

import abc
import logging
from functools import lru_cache
from typing import Any, Dict, Iterable, List, Optional, Type, Union, cast

import singer
import sqlalchemy

from singer_sdk import typing as th
from singer_sdk.exceptions import ConfigValidationError
from singer_sdk.helpers._singer import CatalogEntry, MetadataMapping
from singer_sdk.plugin_base import PluginBase as TapBaseClass
from singer_sdk.streams.core import Stream


class SQLConnector:
    """Base class for SQLAlchemy-based connectors.

    The connector class serves as a wrapper around the SQL connection.

    The functions of the connector are:

    - connecting to the source
    - generating SQLAlchemy connection and engine objects
    - discovering schema catalog entries
    - performing type conversions to/from JSONSchema types
    - dialect-specific functions, such as escaping and fully qualified names
    """

    def __init__(
        self, config: Optional[dict] = None, sqlalchemy_url: Optional[str] = None
    ) -> None:
        """Initialize the SQL connector.

        Args:
            config: The parent tap or target object's config.
            sqlalchemy_url: Optional URL for the connection.
        """
        self._config: Dict[str, Any] = config or {}
        self._sqlalchemy_url: Optional[str] = sqlalchemy_url or None
        self._connection: Optional[sqlalchemy.engine.Connection] = None

    @property
    def config(self) -> dict:
        """If set, provides access to the tap or target config.

        Returns:
            The settings as a dict.
        """
        return self._config

    @property
    def logger(self) -> logging.Logger:
        """Get logger.

        Returns:
            Plugin logger.
        """
        return logging.getLogger("sqlconnector")

    def create_sqlalchemy_connection(self) -> sqlalchemy.engine.Connection:
        """Return a new SQLAlchemy connection using the provided config.

        By default this will create using the sqlalchemy `stream_results=True` option
        described here:
        https://docs.sqlalchemy.org/en/14/core/connections.html#using-server-side-cursors-a-k-a-stream-results

        Developers may override this method if their provider does not support
        server side cursors (`stream_results`) or in order to use different
        configurations options when creating the connection object.

        Returns:
            A newly created SQLAlchemy engine object.
        """
        return (
            self.create_sqlalchemy_engine()
            .connect()
            .execution_options(stream_results=True)
        )

    def create_sqlalchemy_engine(self) -> sqlalchemy.engine.Engine:
        """Return a new SQLAlchemy engine using the provided config.

        Developers can generally override just one of the following:
        `sqlalchemy_engine`, sqlalchemy_url`.

        Returns:
            A newly created SQLAlchemy engine object.
        """
        return sqlalchemy.create_engine(self.sqlalchemy_url)

    @property
    def connection(self) -> sqlalchemy.engine.Connection:
        """Return or set the SQLAlchemy connection object.

        Returns:
            The active SQLAlchemy connection object.
        """
        if not self._connection:
            self._connection = self.create_sqlalchemy_connection()

        return self._connection

    @property
    def sqlalchemy_url(self) -> str:
        """Return the SQLAlchemy URL string.

        Returns:
            The URL as a string.
        """
        if not self._sqlalchemy_url:
            self._sqlalchemy_url = self.get_sqlalchemy_url(self.config)

        return self._sqlalchemy_url

    def get_sqlalchemy_url(self, config: Dict[str, Any]) -> str:
        """Return the SQLAlchemy URL string.

        Developers can generally override just one of the following:
        `sqlalchemy_engine`, `get_sqlalchemy_url`.

        Args:
            config: A dictionary of settings from the tap or target config.

        Returns:
            The URL as a string.

        Raises:
            ConfigValidationError: If no valid sqlalchemy_url can be found.
        """
        if "sqlalchemy_url" not in config:
            raise ConfigValidationError(
                "Could not find or create 'sqlalchemy_url' for connection."
            )

        return cast(str, config["sqlalchemy_url"])

    @staticmethod
    def to_jsonschema_type(
        sql_type: Union[str, Type[sqlalchemy.types.TypeEngine], Any]
    ) -> dict:
        """Return a JSON Schema representation of the provided type.

        By default will call `typing.to_jsonschema_type()` for strings and SQLAlchemy
        types.

        Developers may override this method to accept additional input argument types,
        to support non-standard types, or to provide custom typing logic.

        Args:
            sql_type: The string representation of the SQL type, a SQLAlchemy
                TypeEngine, or a custom-specified object.

        Raises:
            ValueError: If the type received could not be translated to jsonschema.

        Returns:
            The JSON Schema representation of the provided type.
        """
        if isinstance(sql_type, (str,)):
            return th.to_jsonschema_type(sql_type)
        if isinstance(sql_type, type) and issubclass(
            sql_type, sqlalchemy.types.TypeEngine
        ):
            return th.to_jsonschema_type(sql_type)

        raise ValueError(f"Unexpected type received: '{type(sql_type).__name__}'")

    @staticmethod
    def to_sql_type(jsonschema_type: dict) -> sqlalchemy.types.TypeEngine:
        """Return a JSON Schema representation of the provided type.

        By default will call `typing.to_sql_type()`.

        Developers may override this method to accept additional input argument types,
        to support non-standard types, or to provide custom typing logic.

        If overriding this method, developers should call the default implementation
        from the base class for all unhandled cases.

        Args:
            jsonschema_type: The JSON Schema representation of the source type.

        Returns:
            The SQLAlchemy type representation of the data type.
        """
        return th.to_sql_type(jsonschema_type)

    @staticmethod
    def get_fully_qualified_name(
        table_name: str,
        schema_name: Optional[str] = None,
        db_name: Optional[str] = None,
        delimiter: str = ".",
    ) -> str:
        """Concatenates a fully qualified name from the parts.

        Args:
            table_name: The name of the table.
            schema_name: The name of the schema. Defaults to None.
            db_name: The name of the database. Defaults to None.
            delimiter: Generally: '.' for SQL names and '-' for Singer names.

        Raises:
            ValueError: If neither schema_name or db_name are provided.

        Returns:
            The fully qualified name as a string.
        """
        if db_name and schema_name:
            result = delimiter.join([db_name, schema_name, table_name])
        elif db_name:
            result = delimiter.join([db_name, table_name])
        elif schema_name:
            result = delimiter.join([schema_name, table_name])
        else:
            raise ValueError(
                "Schema name or database name was expected for stream: "
                + ":".join(
                    [
                        db_name or "(unknown-db)",
                        schema_name or "(unknown-schema)",
                        table_name,
                    ]
                )
            )

        return result

    @property
    def _dialect(self) -> sqlalchemy.engine.Dialect:
        """Return the dialect object.

        Returns:
            The dialect object.
        """
        return cast(sqlalchemy.engine.Dialect, self.connection.engine.dialect)

    def quote(self, name: str) -> str:
        """Quote a name if it needs quoting.

        Args:
            name: The unquoted name.

        Returns:
            str: The quoted name.
        """
        return cast(str, self._dialect.identifier_preparer.quote(name))

    @lru_cache()
    def _warn_no_view_detection(self) -> None:
        """Print a warning, but only the first time."""
        self.logger.warning(
            "Provider does not support get_view_names(). "
            "Streams list may be incomplete or `is_view` may be unpopulated."
        )

    def discover_catalog_entries(self) -> List[dict]:
        """Return a list of catalog entries from discovery.

        Returns:
            The discovered catalog entries as a list.
        """
        result: List[dict] = []
        engine = self.create_sqlalchemy_engine()
        inspected = sqlalchemy.inspect(engine)
        for schema_name in inspected.get_schema_names():
            # Get list of tables and views
            table_names = inspected.get_table_names(schema=schema_name)
            try:
                view_names = inspected.get_view_names(schema=schema_name)
            except NotImplementedError:
                # Some DB providers do not understand 'views'
                self._warn_no_view_detection()
                view_names = []
            object_names = [(t, False) for t in table_names] + [
                (v, True) for v in view_names
            ]

            # Iterate through each table and view
            for table_name, is_view in object_names:
                # Initialize unique stream name
                unique_stream_id = self.get_fully_qualified_name(
                    db_name=None,
                    schema_name=schema_name,
                    table_name=table_name,
                    delimiter="-",
                )

                # Detect key properties
                possible_primary_keys: List[List[str]] = []
                pk_def = inspected.get_pk_constraint(table_name, schema=schema_name)
                if pk_def and "constrained_columns" in pk_def:
                    possible_primary_keys.append(pk_def["constrained_columns"])
                for index_def in inspected.get_indexes(table_name, schema=schema_name):
                    if index_def.get("unique", False):
                        possible_primary_keys.append(index_def["column_names"])
                key_properties = next(iter(possible_primary_keys), None)

                # Initialize columns list
                table_schema = th.PropertiesList()
                for column_def in inspected.get_columns(table_name, schema=schema_name):
                    column_name = column_def["name"]
                    is_nullable = column_def.get("nullable", False)
                    jsonschema_type: dict = self.to_jsonschema_type(
                        cast(
                            sqlalchemy.types.TypeEngine, column_def["type"]
                        ).python_type
                    )
                    table_schema.append(
                        th.Property(
                            name=column_name,
                            wrapped=th.CustomType(jsonschema_type),
                            required=not is_nullable,
                        )
                    )
                schema = table_schema.to_dict()

                # Initialize available replication methods
                addl_replication_methods: List[str] = [""]  # By default an empty list.
                # Notes regarding replication methods:
                # - 'INCREMENTAL' replication must be enabled by the user by specifying
                #   a replication_key value.
                # - 'LOG_BASED' replication must be enabled by the developer, according
                #   to source-specific implementation capabilities.
                replication_method = next(
                    reversed(["FULL_TABLE"] + addl_replication_methods)
                )

                # Create the catalog entry object
                catalog_entry = CatalogEntry(
                    tap_stream_id=unique_stream_id,
                    stream=unique_stream_id,
                    table=table_name,
                    key_properties=key_properties,
                    schema=singer.Schema.from_dict(schema),
                    is_view=is_view,
                    replication_method=replication_method,
                    metadata=MetadataMapping.get_standard_metadata(
                        schema_name=schema_name,
                        schema=schema,
                        replication_method=replication_method,
                        key_properties=key_properties,
                        valid_replication_keys=None,  # Must be defined by user
                    ),
                    database=None,  # Expects single-database context
                    row_count=None,
                    stream_alias=None,
                    replication_key=None,  # Must be defined by user
                )
                result.append(catalog_entry.to_dict())

        return result


class SQLStream(Stream, metaclass=abc.ABCMeta):
    """Base class for SQLAlchemy-based streams."""

    connector_class = SQLConnector

    def __init__(
        self,
        tap: TapBaseClass,
        catalog_entry: dict,
        connector: Optional[SQLConnector] = None,
    ) -> None:
        """Initialize the database stream.

        If `connector` is omitted, a new connector will be created.

        Args:
            tap: The parent tap object.
            catalog_entry: Catalog entry dict.
            connector: Optional connector to reuse.
        """
        self._connector: SQLConnector
        if connector:
            self._connector = connector
        else:
            self._connector = self.connector_class(dict(tap.config))

        self.catalog_entry = catalog_entry
        super().__init__(
            tap=tap,
            schema=self.schema,
            name=self.tap_stream_id,
        )

    @property
    def _singer_catalog_entry(self) -> CatalogEntry:
        """Return catalog entry as specified by the Singer catalog spec.

        Returns:
            A CatalogEntry object.
        """
        return cast(CatalogEntry, CatalogEntry.from_dict(self.catalog_entry))

    @property
    def connector(self) -> SQLConnector:
        """The connector object.

        Returns:
            The connector object.
        """
        return self._connector

    @property
    def metadata(self) -> MetadataMapping:
        """The Singer metadata.

        Metadata from an input catalog will override standard metadata.

        Returns:
            Metadata object as specified in the Singer spec.
        """
        return self._singer_catalog_entry.metadata

    @property
    def schema(self) -> dict:
        """Return metadata object (dict) as specified in the Singer spec.

        Metadata from an input catalog will override standard metadata.

        Returns:
            The schema object.
        """
        return cast(dict, self._singer_catalog_entry.schema.to_dict())

    @property
    def tap_stream_id(self) -> str:
        """Return the unique ID used by the tap to identify this stream.

        Generally, this is the same value as in `Stream.name`.

        In rare cases, such as for database types with multi-part names,
        this may be slightly different from `Stream.name`.

        Returns:
            The unique tap stream ID as a string.
        """
        return self._singer_catalog_entry.tap_stream_id

    @property
    def primary_keys(self) -> Optional[List[str]]:
        """Get primary keys from the catalog entry definition.

        Returns:
            A list of primary key(s) for the stream.
        """
        return self._singer_catalog_entry.metadata.root.table_key_properties or []

    @primary_keys.setter
    def primary_keys(self, new_value: List[str]) -> None:
        """Set or reset the primary key(s) in the stream's catalog entry.

        Args:
            new_value: a list of one or more column names
        """
        self._singer_catalog_entry.metadata.root.table_key_properties = new_value

    @property
    def fully_qualified_name(self) -> str:
        """Generate the fully qualified version of the table name.

        Raises:
            ValueError: If table_name is not able to be detected.

        Returns:
            The fully qualified name.
        """
        catalog_entry = self._singer_catalog_entry
        if not catalog_entry.table:
            raise ValueError(
                f"Missing table name in catalog entry: {catalog_entry.to_dict()}"
            )

        return self.connector.get_fully_qualified_name(
            table_name=catalog_entry.table,
            schema_name=catalog_entry.metadata.root.schema_name,
            db_name=catalog_entry.database,
        )

    # Get records from stream

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        """Return a generator of row-type dictionary objects.

        If the stream has a replication_key value defined, records will be sorted by the
        incremental key. If the stream also has an available starting bookmark, the
        records will be filtered for values greater than or equal to the bookmark value.

        Args:
            context: If partition context is provided, will read specifically from this
                data slice.

        Yields:
            One dict per record.

        Raises:
            NotImplementedError: If partition is passed in context and the stream does
                not support partitioning.
        """
        if context:
            raise NotImplementedError(
                f"Stream '{self.name}' does not support partitioning."
            )

        query_text = sqlalchemy.text(f"SELECT * FROM {self.fully_qualified_name}")
        if self.replication_key:
            quoted_replication_key = self.connector.quote(self.replication_key)
            query_text += sqlalchemy.text(f"\nORDER BY {quoted_replication_key}")

            start_val = self.get_starting_replication_key_value(context)
            if start_val:
                query_text += sqlalchemy.text(
                    f"\nWHERE {quoted_replication_key} >= :start_val"
                ).bindparams(start_val=start_val)

        for row in self.connector.connection.execute(query_text):
            yield dict(row)
