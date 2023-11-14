"""Common SQL connectors for Streams and Sinks."""

from __future__ import annotations

import decimal
import json
import logging
import typing as t
import warnings
from contextlib import contextmanager
from datetime import datetime
from functools import lru_cache

import simplejson
import sqlalchemy
from sqlalchemy.engine import Engine

from singer_sdk import typing as th
from singer_sdk._singerlib import CatalogEntry, MetadataMapping, Schema
from singer_sdk.exceptions import ConfigValidationError
from singer_sdk.helpers.capabilities import TargetLoadMethods

if t.TYPE_CHECKING:
    from sqlalchemy.engine.reflection import Inspector


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

    allow_column_add: bool = True  # Whether ADD COLUMN is supported.
    allow_column_rename: bool = True  # Whether RENAME COLUMN is supported.
    allow_column_alter: bool = False  # Whether altering column types is supported.
    allow_merge_upsert: bool = False  # Whether MERGE UPSERT is supported.
    allow_overwrite: bool = False  # Whether overwrite load method is supported.
    allow_temp_tables: bool = True  # Whether temp tables are supported.
    _cached_engine: Engine | None = None

    def __init__(
        self,
        config: dict | None = None,
        sqlalchemy_url: str | None = None,
    ) -> None:
        """Initialize the SQL connector.

        Args:
            config: The parent tap or target object's config.
            sqlalchemy_url: Optional URL for the connection.
        """
        self._config: dict[str, t.Any] = config or {}
        self._sqlalchemy_url: str | None = sqlalchemy_url or None
        self._table_cols_cache: dict[str, dict[str, sqlalchemy.Column]] = {}
        self._schema_cache: set[str] = set()

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

    @contextmanager
    def _connect(self) -> t.Iterator[sqlalchemy.engine.Connection]:
        with self._engine.connect().execution_options(stream_results=True) as conn:
            yield conn

    def create_sqlalchemy_connection(self) -> sqlalchemy.engine.Connection:
        """(DEPRECATED) Return a new SQLAlchemy connection using the provided config.

        Do not use the SQLConnector's connection directly. Instead, if you need
        to execute something that isn't available on the connector currently,
        make a child class and add a method on that connector.

        By default this will create using the sqlalchemy `stream_results=True` option
        described here:

        https://docs.sqlalchemy.org/en/14/core/connections.html#using-server-side-cursors-a-k-a-stream-results

        Developers may override this method if their provider does not support
        server side cursors (`stream_results`) or in order to use different
        configurations options when creating the connection object.

        Returns:
            A newly created SQLAlchemy engine object.
        """
        warnings.warn(
            "`SQLConnector.create_sqlalchemy_connection` is deprecated. "
            "If you need to execute something that isn't available "
            "on the connector currently, make a child class and "
            "add your required method on that connector.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self._engine.connect().execution_options(stream_results=True)

    def create_sqlalchemy_engine(self) -> Engine:
        """(DEPRECATED) Return a new SQLAlchemy engine using the provided config.

        Developers can generally override just one of the following:
        `sqlalchemy_engine`, sqlalchemy_url`.

        Returns:
            A newly created SQLAlchemy engine object.
        """
        warnings.warn(
            "`SQLConnector.create_sqlalchemy_engine` is deprecated. Override"
            "`_engine` or sqlalchemy_url` instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self._engine

    @property
    def connection(self) -> sqlalchemy.engine.Connection:
        """(DEPRECATED) Return or set the SQLAlchemy connection object.

        Do not use the SQLConnector's connection directly. Instead, if you need
        to execute something that isn't available on the connector currently,
        make a child class and add a method on that connector.

        Returns:
            The active SQLAlchemy connection object.
        """
        warnings.warn(
            "`SQLConnector.connection` is deprecated. If you need to execute something "
            "that isn't available on the connector currently, make a child "
            "class and add your required method on that connector.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.create_sqlalchemy_connection()

    @property
    def sqlalchemy_url(self) -> str:
        """Return the SQLAlchemy URL string.

        Returns:
            The URL as a string.
        """
        if not self._sqlalchemy_url:
            self._sqlalchemy_url = self.get_sqlalchemy_url(self.config)

        return self._sqlalchemy_url

    def get_sqlalchemy_url(self, config: dict[str, t.Any]) -> str:
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
            msg = "Could not find or create 'sqlalchemy_url' for connection."
            raise ConfigValidationError(msg)

        return t.cast(str, config["sqlalchemy_url"])

    @staticmethod
    def to_jsonschema_type(
        sql_type: (
            str  # noqa: ANN401
            | sqlalchemy.types.TypeEngine
            | type[sqlalchemy.types.TypeEngine]
            | t.Any
        ),
    ) -> dict:
        """Return a JSON Schema representation of the provided type.

        By default will call `typing.to_jsonschema_type()` for strings and SQLAlchemy
        types.

        Developers may override this method to accept additional input argument types,
        to support non-standard types, or to provide custom typing logic.

        Args:
            sql_type: The string representation of the SQL type, a SQLAlchemy
                TypeEngine class or object, or a custom-specified object.

        Raises:
            ValueError: If the type received could not be translated to jsonschema.

        Returns:
            The JSON Schema representation of the provided type.
        """
        if isinstance(sql_type, (str, sqlalchemy.types.TypeEngine)):
            return th.to_jsonschema_type(sql_type)

        if isinstance(sql_type, type):
            if issubclass(sql_type, sqlalchemy.types.TypeEngine):
                return th.to_jsonschema_type(sql_type)

            msg = f"Unexpected type received: '{sql_type.__name__}'"
            raise ValueError(msg)

        msg = f"Unexpected type received: '{type(sql_type).__name__}'"
        raise ValueError(msg)

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
        table_name: str | None = None,
        schema_name: str | None = None,
        db_name: str | None = None,
        delimiter: str = ".",
    ) -> str:
        """Concatenates a fully qualified name from the parts.

        Args:
            table_name: The name of the table.
            schema_name: The name of the schema. Defaults to None.
            db_name: The name of the database. Defaults to None.
            delimiter: Generally: '.' for SQL names and '-' for Singer names.

        Raises:
            ValueError: If all 3 name parts not supplied.

        Returns:
            The fully qualified name as a string.
        """
        parts = []

        if db_name:
            parts.append(db_name)
        if schema_name:
            parts.append(schema_name)
        if table_name:
            parts.append(table_name)

        if not parts:
            raise ValueError(
                "Could not generate fully qualified name: "
                + ":".join(
                    [
                        db_name or "(unknown-db)",
                        schema_name or "(unknown-schema)",
                        table_name or "(unknown-table-name)",
                    ],
                ),
            )

        return delimiter.join(parts)

    @property
    def _dialect(self) -> sqlalchemy.engine.Dialect:
        """Return the dialect object.

        Returns:
            The dialect object.
        """
        return t.cast(sqlalchemy.engine.Dialect, self._engine.dialect)

    @property
    def _engine(self) -> Engine:
        """Return the engine object.

        This is the correct way to access the Connector's engine, if needed
        (e.g. to inspect tables).

        Returns:
            The SQLAlchemy Engine that's attached to this SQLConnector instance.
        """
        if not self._cached_engine:
            self._cached_engine = self.create_engine()
        return t.cast(Engine, self._cached_engine)

    def create_engine(self) -> Engine:
        """Creates and returns a new engine. Do not call outside of _engine.

        NOTE: Do not call this method. The only place that this method should
        be called is inside the self._engine method. If you'd like to access
        the engine on a connector, use self._engine.

        This method exists solely so that tap/target developers can override it
        on their subclass of SQLConnector to perform custom engine creation
        logic.

        Returns:
            A new SQLAlchemy Engine.
        """
        try:
            return sqlalchemy.create_engine(
                self.sqlalchemy_url,
                echo=False,
                json_serializer=self.serialize_json,
                json_deserializer=self.deserialize_json,
            )
        except TypeError:
            self.logger.exception(
                "Retrying engine creation with fewer arguments due to TypeError.",
            )
            return sqlalchemy.create_engine(
                self.sqlalchemy_url,
                echo=False,
            )

    def quote(self, name: str) -> str:
        """Quote a name if it needs quoting, using '.' as a name-part delimiter.

        Examples:
          "my_table"           => "`my_table`"
          "my_schema.my_table" => "`my_schema`.`my_table`"

        Args:
            name: The unquoted name.

        Returns:
            str: The quoted name.
        """
        return ".".join(
            [
                self._dialect.identifier_preparer.quote(name_part)
                for name_part in name.split(".")
            ],
        )

    @lru_cache()  # noqa: B019
    def _warn_no_view_detection(self) -> None:
        """Print a warning, but only the first time."""
        self.logger.warning(
            "Provider does not support get_view_names(). "
            "Streams list may be incomplete or `is_view` may be unpopulated.",
        )

    def get_schema_names(
        self,
        engine: Engine,  # noqa: ARG002
        inspected: Inspector,
    ) -> list[str]:
        """Return a list of schema names in DB.

        Args:
            engine: SQLAlchemy engine
            inspected: SQLAlchemy inspector instance for engine

        Returns:
            List of schema names
        """
        return inspected.get_schema_names()

    def get_object_names(
        self,
        engine: Engine,  # noqa: ARG002
        inspected: Inspector,
        schema_name: str,
    ) -> list[tuple[str, bool]]:
        """Return a list of syncable objects.

        Args:
            engine: SQLAlchemy engine
            inspected: SQLAlchemy inspector instance for engine
            schema_name: Schema name to inspect

        Returns:
            List of tuples (<table_or_view_name>, <is_view>)
        """
        # Get list of tables and views
        table_names = inspected.get_table_names(schema=schema_name)
        try:
            view_names = inspected.get_view_names(schema=schema_name)
        except NotImplementedError:
            # Some DB providers do not understand 'views'
            self._warn_no_view_detection()
            view_names = []
        return [(t, False) for t in table_names] + [(v, True) for v in view_names]

    # TODO maybe should be splitted into smaller parts?
    def discover_catalog_entry(
        self,
        engine: Engine,  # noqa: ARG002
        inspected: Inspector,
        schema_name: str,
        table_name: str,
        is_view: bool,  # noqa: FBT001
    ) -> CatalogEntry:
        """Create `CatalogEntry` object for the given table or a view.

        Args:
            engine: SQLAlchemy engine
            inspected: SQLAlchemy inspector instance for engine
            schema_name: Schema name to inspect
            table_name: Name of the table or a view
            is_view: Flag whether this object is a view, returned by `get_object_names`

        Returns:
            `CatalogEntry` object for the given table or a view
        """
        # Initialize unique stream name
        unique_stream_id = self.get_fully_qualified_name(
            db_name=None,
            schema_name=schema_name,
            table_name=table_name,
            delimiter="-",
        )

        # Detect key properties
        possible_primary_keys: list[list[str]] = []
        pk_def = inspected.get_pk_constraint(table_name, schema=schema_name)
        if pk_def and "constrained_columns" in pk_def:
            possible_primary_keys.append(pk_def["constrained_columns"])

        possible_primary_keys.extend(
            index_def["column_names"]
            for index_def in inspected.get_indexes(table_name, schema=schema_name)
            if index_def.get("unique", False)
        )

        key_properties = next(iter(possible_primary_keys), None)

        # Initialize columns list
        table_schema = th.PropertiesList()
        for column_def in inspected.get_columns(table_name, schema=schema_name):
            column_name = column_def["name"]
            is_nullable = column_def.get("nullable", False)
            jsonschema_type: dict = self.to_jsonschema_type(
                t.cast(sqlalchemy.types.TypeEngine, column_def["type"]),
            )
            table_schema.append(
                th.Property(
                    name=column_name,
                    wrapped=th.CustomType(jsonschema_type),
                    required=not is_nullable,
                ),
            )
        schema = table_schema.to_dict()

        # Initialize available replication methods
        addl_replication_methods: list[str] = [""]  # By default an empty list.
        # Notes regarding replication methods:
        # - 'INCREMENTAL' replication must be enabled by the user by specifying
        #   a replication_key value.
        # - 'LOG_BASED' replication must be enabled by the developer, according
        #   to source-specific implementation capabilities.
        replication_method = next(reversed(["FULL_TABLE", *addl_replication_methods]))

        # Create the catalog entry object
        return CatalogEntry(
            tap_stream_id=unique_stream_id,
            stream=unique_stream_id,
            table=table_name,
            key_properties=key_properties,
            schema=Schema.from_dict(schema),
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

    def discover_catalog_entries(self) -> list[dict]:
        """Return a list of catalog entries from discovery.

        Returns:
            The discovered catalog entries as a list.
        """
        result: list[dict] = []
        engine = self._engine
        inspected = sqlalchemy.inspect(engine)
        for schema_name in self.get_schema_names(engine, inspected):
            # Iterate through each table and view
            for table_name, is_view in self.get_object_names(
                engine,
                inspected,
                schema_name,
            ):
                catalog_entry = self.discover_catalog_entry(
                    engine,
                    inspected,
                    schema_name,
                    table_name,
                    is_view,
                )
                result.append(catalog_entry.to_dict())

        return result

    def parse_full_table_name(
        self,
        full_table_name: str,
    ) -> tuple[str | None, str | None, str]:
        """Parse a fully qualified table name into its parts.

        Developers may override this method if their platform does not support the
        traditional 3-part convention: `db_name.schema_name.table_name`

        Args:
            full_table_name: A table name or a fully qualified table name. Depending on
                SQL the platform, this could take the following forms:
                - `<db>.<schema>.<table>` (three part names)
                - `<db>.<table>` (platforms which do not use schema groupings)
                - `<schema>.<name>` (if DB name is already in context)
                - `<table>` (if DB name and schema name are already in context)

        Returns:
            A three part tuple (db_name, schema_name, table_name) with any unspecified
            or unused parts returned as None.
        """
        db_name: str | None = None
        schema_name: str | None = None

        parts = full_table_name.split(".")
        if len(parts) == 1:
            table_name = full_table_name
        if len(parts) == 2:  # noqa: PLR2004
            schema_name, table_name = parts
        if len(parts) == 3:  # noqa: PLR2004
            db_name, schema_name, table_name = parts

        return db_name, schema_name, table_name

    def table_exists(self, full_table_name: str) -> bool:
        """Determine if the target table already exists.

        Args:
            full_table_name: the target table name.

        Returns:
            True if table exists, False if not, None if unsure or undetectable.
        """
        _, schema_name, table_name = self.parse_full_table_name(full_table_name)

        return t.cast(
            bool,
            sqlalchemy.inspect(self._engine).has_table(table_name, schema_name),
        )

    def schema_exists(self, schema_name: str) -> bool:
        """Determine if the target database schema already exists.

        Args:
            schema_name: The target database schema name.

        Returns:
            True if the database schema exists, False if not.
        """
        if schema_name not in self._schema_cache:
            self._schema_cache = set(
                sqlalchemy.inspect(self._engine).get_schema_names(),
            )

        return schema_name in self._schema_cache

    def get_table_columns(
        self,
        full_table_name: str,
        column_names: list[str] | None = None,
    ) -> dict[str, sqlalchemy.Column]:
        """Return a list of table columns.

        Args:
            full_table_name: Fully qualified table name.
            column_names: A list of column names to filter to.

        Returns:
            An ordered list of column objects.
        """
        if full_table_name not in self._table_cols_cache:
            _, schema_name, table_name = self.parse_full_table_name(full_table_name)
            inspector = sqlalchemy.inspect(self._engine)
            columns = inspector.get_columns(table_name, schema_name)

            self._table_cols_cache[full_table_name] = {
                col_meta["name"]: sqlalchemy.Column(
                    col_meta["name"],
                    col_meta["type"],
                    nullable=col_meta.get("nullable", False),
                )
                for col_meta in columns
                if not column_names
                or col_meta["name"].casefold()
                in {col.casefold() for col in column_names}
            }

        return self._table_cols_cache[full_table_name]

    def get_table(
        self,
        full_table_name: str,
        column_names: list[str] | None = None,
    ) -> sqlalchemy.Table:
        """Return a table object.

        Args:
            full_table_name: Fully qualified table name.
            column_names: A list of column names to filter to.

        Returns:
            A table object with column list.
        """
        columns = self.get_table_columns(
            full_table_name=full_table_name,
            column_names=column_names,
        ).values()
        _, schema_name, table_name = self.parse_full_table_name(full_table_name)
        meta = sqlalchemy.MetaData()
        return sqlalchemy.schema.Table(
            table_name,
            meta,
            *list(columns),
            schema=schema_name,
        )

    def column_exists(self, full_table_name: str, column_name: str) -> bool:
        """Determine if the target table already exists.

        Args:
            full_table_name: the target table name.
            column_name: the target column name.

        Returns:
            True if table exists, False if not.
        """
        return column_name in self.get_table_columns(full_table_name)

    def create_schema(self, schema_name: str) -> None:
        """Create target schema.

        Args:
            schema_name: The target schema to create.
        """
        with self._connect() as conn, conn.begin():
            conn.execute(sqlalchemy.schema.CreateSchema(schema_name))

    def create_empty_table(
        self,
        full_table_name: str,
        schema: dict,
        primary_keys: list[str] | None = None,
        partition_keys: list[str] | None = None,
        as_temp_table: bool = False,  # noqa: FBT001, FBT002
    ) -> None:
        """Create an empty target table.

        Args:
            full_table_name: the target table name.
            schema: the JSON schema for the new table.
            primary_keys: list of key properties.
            partition_keys: list of partition keys.
            as_temp_table: True to create a temp table.

        Raises:
            NotImplementedError: if temp tables are unsupported and as_temp_table=True.
            RuntimeError: if a variant schema is passed with no properties defined.
        """
        if as_temp_table:
            msg = "Temporary tables are not supported."
            raise NotImplementedError(msg)

        _ = partition_keys  # Not supported in generic implementation.

        _, schema_name, table_name = self.parse_full_table_name(full_table_name)
        meta = sqlalchemy.MetaData(schema=schema_name)
        columns: list[sqlalchemy.Column] = []
        primary_keys = primary_keys or []
        try:
            properties: dict = schema["properties"]
        except KeyError as e:
            msg = f"Schema for '{full_table_name}' does not define properties: {schema}"
            raise RuntimeError(msg) from e
        for property_name, property_jsonschema in properties.items():
            is_primary_key = property_name in primary_keys
            columns.append(
                sqlalchemy.Column(
                    property_name,
                    self.to_sql_type(property_jsonschema),
                    primary_key=is_primary_key,
                ),
            )

        _ = sqlalchemy.Table(table_name, meta, *columns)
        meta.create_all(self._engine)

    def _create_empty_column(
        self,
        full_table_name: str,
        column_name: str,
        sql_type: sqlalchemy.types.TypeEngine,
    ) -> None:
        """Create a new column.

        Args:
            full_table_name: The target table name.
            column_name: The name of the new column.
            sql_type: SQLAlchemy type engine to be used in creating the new column.

        Raises:
            NotImplementedError: if adding columns is not supported.
        """
        if not self.allow_column_add:
            msg = "Adding columns is not supported."
            raise NotImplementedError(msg)

        column_add_ddl = self.get_column_add_ddl(
            table_name=full_table_name,
            column_name=column_name,
            column_type=sql_type,
        )
        with self._connect() as conn, conn.begin():
            conn.execute(column_add_ddl)

    def prepare_schema(self, schema_name: str) -> None:
        """Create the target database schema.

        Args:
            schema_name: The target schema name.
        """
        schema_exists = self.schema_exists(schema_name)
        if not schema_exists:
            self.create_schema(schema_name)

    def prepare_table(
        self,
        full_table_name: str,
        schema: dict,
        primary_keys: list[str],
        partition_keys: list[str] | None = None,
        as_temp_table: bool = False,  # noqa: FBT002, FBT001
    ) -> None:
        """Adapt target table to provided schema if possible.

        Args:
            full_table_name: the target table name.
            schema: the JSON Schema for the table.
            primary_keys: list of key properties.
            partition_keys: list of partition keys.
            as_temp_table: True to create a temp table.
        """
        if not self.table_exists(full_table_name=full_table_name):
            self.create_empty_table(
                full_table_name=full_table_name,
                schema=schema,
                primary_keys=primary_keys,
                partition_keys=partition_keys,
                as_temp_table=as_temp_table,
            )
            return
        if self.config["load_method"] == TargetLoadMethods.OVERWRITE:
            self.get_table(full_table_name=full_table_name).drop(self._engine)
            self.create_empty_table(
                full_table_name=full_table_name,
                schema=schema,
                primary_keys=primary_keys,
                partition_keys=partition_keys,
                as_temp_table=as_temp_table,
            )
            return

        for property_name, property_def in schema["properties"].items():
            self.prepare_column(
                full_table_name,
                property_name,
                self.to_sql_type(property_def),
            )

    def prepare_column(
        self,
        full_table_name: str,
        column_name: str,
        sql_type: sqlalchemy.types.TypeEngine,
    ) -> None:
        """Adapt target table to provided schema if possible.

        Args:
            full_table_name: the target table name.
            column_name: the target column name.
            sql_type: the SQLAlchemy type.
        """
        if not self.column_exists(full_table_name, column_name):
            self._create_empty_column(
                full_table_name=full_table_name,
                column_name=column_name,
                sql_type=sql_type,
            )
            return

        self._adapt_column_type(
            full_table_name,
            column_name=column_name,
            sql_type=sql_type,
        )

    def rename_column(self, full_table_name: str, old_name: str, new_name: str) -> None:
        """Rename the provided columns.

        Args:
            full_table_name: The fully qualified table name.
            old_name: The old column to be renamed.
            new_name: The new name for the column.

        Raises:
            NotImplementedError: If `self.allow_column_rename` is false.
        """
        if not self.allow_column_rename:
            msg = "Renaming columns is not supported."
            raise NotImplementedError(msg)

        column_rename_ddl = self.get_column_rename_ddl(
            table_name=full_table_name,
            column_name=old_name,
            new_column_name=new_name,
        )
        with self._connect() as conn, conn.begin():
            conn.execute(column_rename_ddl)

    def merge_sql_types(
        self,
        sql_types: list[sqlalchemy.types.TypeEngine],
    ) -> sqlalchemy.types.TypeEngine:
        """Return a compatible SQL type for the selected type list.

        Args:
            sql_types: List of SQL types.

        Returns:
            A SQL type that is compatible with the input types.

        Raises:
            ValueError: If sql_types argument has zero members.
        """
        if not sql_types:
            msg = "Expected at least one member in `sql_types` argument."
            raise ValueError(msg)

        if len(sql_types) == 1:
            return sql_types[0]

        # Gathering Type to match variables
        # sent in _adapt_column_type
        current_type = sql_types[0]
        cur_len: int = getattr(current_type, "length", 0)

        # Convert the two types given into a sorted list
        # containing the best conversion classes
        sql_types = self._sort_types(sql_types)

        # If greater than two evaluate the first pair then on down the line
        if len(sql_types) > 2:  # noqa: PLR2004
            return self.merge_sql_types(
                [self.merge_sql_types([sql_types[0], sql_types[1]])] + sql_types[2:],
            )

        # Get the generic type class
        for opt in sql_types:
            # Get the length
            opt_len: int = getattr(opt, "length", 0)
            generic_type = type(opt.as_generic())

            if isinstance(generic_type, type):
                if issubclass(
                    generic_type,
                    (sqlalchemy.types.String, sqlalchemy.types.Unicode),
                ) or issubclass(
                    generic_type,
                    (sqlalchemy.types.String, sqlalchemy.types.Unicode),
                ):
                    # If length None or 0 then is varchar max ?
                    if (
                        (opt_len is None)
                        or (opt_len == 0)
                        or (cur_len and (opt_len >= cur_len))
                    ):
                        return opt
                # If best conversion class is equal to current type
                # return the best conversion class
                elif str(opt) == str(current_type):
                    return opt

        msg = f"Unable to merge sql types: {', '.join([str(t) for t in sql_types])}"
        raise ValueError(msg)

    def _sort_types(
        self,
        sql_types: t.Iterable[sqlalchemy.types.TypeEngine],
    ) -> list[sqlalchemy.types.TypeEngine]:
        """Return the input types sorted from most to least compatible.

        For example, [Smallint, Integer, Datetime, String, Double] would become
        [Unicode, String, Double, Integer, Smallint, Datetime].

        String types will be listed first, then decimal types, then integer types,
        then bool types, and finally datetime and date. Higher precision, scale, and
        length will be sorted earlier.

        Args:
            sql_types (List[sqlalchemy.types.TypeEngine]): [description]

        Returns:
            The sorted list.
        """

        def _get_type_sort_key(
            sql_type: sqlalchemy.types.TypeEngine,
        ) -> tuple[int, int]:
            # return rank, with higher numbers ranking first

            _len = int(getattr(sql_type, "length", 0) or 0)

            _pytype = t.cast(type, sql_type.python_type)
            if issubclass(_pytype, (str, bytes)):
                return 900, _len
            if issubclass(_pytype, datetime):
                return 600, _len
            if issubclass(_pytype, float):
                return 400, _len
            if issubclass(_pytype, int):
                return 300, _len

            return 0, _len

        return sorted(sql_types, key=_get_type_sort_key, reverse=True)

    def _get_column_type(
        self,
        full_table_name: str,
        column_name: str,
    ) -> sqlalchemy.types.TypeEngine:
        """Get the SQL type of the declared column.

        Args:
            full_table_name: The name of the table.
            column_name: The name of the column.

        Returns:
            The type of the column.

        Raises:
            KeyError: If the provided column name does not exist.
        """
        try:
            column = self.get_table_columns(full_table_name)[column_name]
        except KeyError as ex:
            msg = f"Column `{column_name}` does not exist in table `{full_table_name}`."
            raise KeyError(msg) from ex

        return t.cast(sqlalchemy.types.TypeEngine, column.type)

    @staticmethod
    def get_column_add_ddl(
        table_name: str,
        column_name: str,
        column_type: sqlalchemy.types.TypeEngine,
    ) -> sqlalchemy.DDL:
        """Get the create column DDL statement.

        Override this if your database uses a different syntax for creating columns.

        Args:
            table_name: Fully qualified table name of column to alter.
            column_name: Column name to create.
            column_type: New column sqlalchemy type.

        Returns:
            A sqlalchemy DDL instance.
        """
        create_column_clause = sqlalchemy.schema.CreateColumn(
            sqlalchemy.Column(
                column_name,
                column_type,
            ),
        )
        return sqlalchemy.DDL(
            "ALTER TABLE %(table_name)s ADD COLUMN %(create_column_clause)s",
            {
                "table_name": table_name,
                "create_column_clause": create_column_clause,
            },
        )

    @staticmethod
    def get_column_rename_ddl(
        table_name: str,
        column_name: str,
        new_column_name: str,
    ) -> sqlalchemy.DDL:
        """Get the create column DDL statement.

        Override this if your database uses a different syntax for renaming columns.

        Args:
            table_name: Fully qualified table name of column to alter.
            column_name: Existing column name.
            new_column_name: New column name.

        Returns:
            A sqlalchemy DDL instance.
        """
        return sqlalchemy.DDL(
            "ALTER TABLE %(table_name)s "
            "RENAME COLUMN %(column_name)s to %(new_column_name)s",
            {
                "table_name": table_name,
                "column_name": column_name,
                "new_column_name": new_column_name,
            },
        )

    @staticmethod
    def get_column_alter_ddl(
        table_name: str,
        column_name: str,
        column_type: sqlalchemy.types.TypeEngine,
    ) -> sqlalchemy.DDL:
        """Get the alter column DDL statement.

        Override this if your database uses a different syntax for altering columns.

        Args:
            table_name: Fully qualified table name of column to alter.
            column_name: Column name to alter.
            column_type: New column type string.

        Returns:
            A sqlalchemy DDL instance.
        """
        return sqlalchemy.DDL(
            "ALTER TABLE %(table_name)s ALTER COLUMN %(column_name)s (%(column_type)s)",
            {
                "table_name": table_name,
                "column_name": column_name,
                "column_type": column_type,
            },
        )

    @staticmethod
    def remove_collation(
        column_type: sqlalchemy.types.TypeEngine,
    ) -> str | None:
        """Removes collation for the given column TypeEngine instance.

        Args:
            column_type: Column SQLAlchemy type.

        Returns:
             The removed collation as a string.
        """
        if hasattr(column_type, "collation") and column_type.collation:
            column_type_collation: str = column_type.collation
            column_type.collation = None
            return column_type_collation
        return None

    @staticmethod
    def update_collation(
        column_type: sqlalchemy.types.TypeEngine,
        collation: str | None,
    ) -> None:
        """Sets column collation if column type has a collation attribute.

        Args:
            column_type: Column SQLAlchemy type.
            collation: The colation
        """
        if hasattr(column_type, "collation") and collation:
            column_type.collation = collation

    def _adapt_column_type(
        self,
        full_table_name: str,
        column_name: str,
        sql_type: sqlalchemy.types.TypeEngine,
    ) -> None:
        """Adapt table column type to support the new JSON schema type.

        Args:
            full_table_name: The target table name.
            column_name: The target column name.
            sql_type: The new SQLAlchemy type.

        Raises:
            NotImplementedError: if altering columns is not supported.
        """
        current_type: sqlalchemy.types.TypeEngine = self._get_column_type(
            full_table_name,
            column_name,
        )

        # remove collation if present and save it
        current_type_collation = self.remove_collation(current_type)

        # Check if the existing column type and the sql type are the same
        if str(sql_type) == str(current_type):
            # The current column and sql type are the same
            # Nothing to do
            return

        # Not the same type, generic type or compatible types
        # calling merge_sql_types for assistnace
        compatible_sql_type = self.merge_sql_types([current_type, sql_type])

        if str(compatible_sql_type) == str(current_type):
            # Nothing to do
            return

        # Put the collation level back before altering the column
        if current_type_collation:
            self.update_collation(compatible_sql_type, current_type_collation)

        if not self.allow_column_alter:
            msg = (
                "Altering columns is not supported. Could not convert column "
                f"'{full_table_name}.{column_name}' from '{current_type}' to "
                f"'{compatible_sql_type}'."
            )
            raise NotImplementedError(msg)

        alter_column_ddl = self.get_column_alter_ddl(
            table_name=full_table_name,
            column_name=column_name,
            column_type=compatible_sql_type,
        )
        with self._connect() as conn, conn.begin():
            conn.execute(alter_column_ddl)

    def serialize_json(self, obj: object) -> str:
        """Serialize an object to a JSON string.

        Target connectors may override this method to provide custom serialization logic
        for JSON types.

        Args:
            obj: The object to serialize.

        Returns:
            The JSON string.

        .. versionadded:: 0.31.0
        """
        return simplejson.dumps(obj, use_decimal=True)

    def deserialize_json(self, json_str: str) -> object:
        """Deserialize a JSON string to an object.

        Tap connectors may override this method to provide custom deserialization
        logic for JSON types.

        Args:
            json_str: The JSON string to deserialize.

        Returns:
            The deserialized object.

        .. versionadded:: 0.31.0
        """
        return json.loads(json_str, parse_float=decimal.Decimal)
