"""Common SQL connectors for Streams and Sinks."""

from __future__ import annotations

import functools
import logging
import sys
import typing as t
import warnings
from collections import UserString
from contextlib import contextmanager
from datetime import datetime
from functools import lru_cache

import sqlalchemy as sa
from sqlalchemy.engine import reflection

from singer_sdk import typing as th
from singer_sdk.exceptions import ConfigValidationError
from singer_sdk.helpers._compat import SingerSDKDeprecationWarning
from singer_sdk.helpers._util import dump_json, load_json
from singer_sdk.helpers.capabilities import TargetLoadMethods
from singer_sdk.singerlib import CatalogEntry, MetadataMapping, Schema

if sys.version_info < (3, 13):
    from typing_extensions import deprecated
else:
    from warnings import deprecated

if sys.version_info < (3, 10):
    from typing_extensions import TypeAlias
else:
    from typing import TypeAlias  # noqa: ICN003

if t.TYPE_CHECKING:
    from sqlalchemy.engine import Engine
    from sqlalchemy.engine.reflection import Inspector


class FullyQualifiedName(UserString):
    """A fully qualified table name.

    This class provides a simple way to represent a fully qualified table name
    as a single object. The string representation of this object is the fully
    qualified table name, with the parts separated by periods.

    The parts of the fully qualified table name are:
    - database
    - schema
    - table

    The database and schema are optional. If only the table name is provided,
    the string representation of the object will be the table name alone.

    Example:
    ```
    table_name = FullyQualifiedName("my_table", "my_schema", "my_db")
    print(table_name)  # my_db.my_schema.my_table
    ```
    """

    def __init__(
        self,
        *,
        table: str = "",
        schema: str | None = None,
        database: str | None = None,
        delimiter: str = ".",
    ) -> None:
        """Initialize the fully qualified table name.

        Args:
            table: The name of the table.
            schema: The name of the schema. Defaults to None.
            database: The name of the database. Defaults to None.
            delimiter: The delimiter to use between parts. Defaults to '.'.

        Raises:
            ValueError: If the fully qualified name could not be generated.
        """
        self.table = table
        self.schema = schema
        self.database = database
        self.delimiter = delimiter

        parts = []
        if self.database:
            parts.append(self.prepare_part(self.database))
        if self.schema:
            parts.append(self.prepare_part(self.schema))
        if self.table:
            parts.append(self.prepare_part(self.table))

        if not parts:
            raise ValueError(
                "Could not generate fully qualified name: "
                + ":".join(
                    [
                        self.database or "(unknown-db)",
                        self.schema or "(unknown-schema)",
                        self.table or "(unknown-table-name)",
                    ],
                ),
            )

        super().__init__(self.delimiter.join(parts))

    def prepare_part(self, part: str) -> str:  # noqa: PLR6301
        """Prepare a part of the fully qualified name.

        Args:
            part: The part to prepare.

        Returns:
            The prepared part.
        """
        return part


class SQLToJSONSchema:
    """SQLAlchemy to JSON Schema type mapping helper.

    This class provides a mapping from SQLAlchemy types to JSON Schema types.

    .. versionadded:: 0.41.0
    .. versionchanged:: 0.43.0
       Added the :meth:`singer_sdk.connectors.sql.SQLToJSONSchema.from_config` class
       method.
    .. versionchanged:: 0.45.0
       Added support for the `use_singer_decimal` option.
    """

    def __init__(self, *, use_singer_decimal: bool = False) -> None:
        """Initialize the SQL to JSON Schema converter.

        Args:
            use_singer_decimal: Whether to represent numbers as `string` with
                the `x-singer.decimal` format instead of as `number`.
        """
        self.use_singer_decimal = use_singer_decimal

    @classmethod
    def from_config(cls: type[SQLToJSONSchema], config: dict) -> SQLToJSONSchema:
        """Create a new instance from a configuration dictionary.

        Override this to instantiate this converter with values from the tap's
        configuration dictionary.

        .. code-block:: python

           class CustomSQLToJSONSchema(SQLToJSONSchema):
               def __init__(self, *, my_custom_option, **kwargs):
                   super().__init__(**kwargs)
                   self.my_custom_option = my_custom_option

               @classmethod
               def from_config(cls, config):
                   return cls(my_custom_option=config.get("my_custom_option"))

        Args:
            config: The configuration dictionary.
            use_singer_decimal: Whether to represent numbers as `string` with
                the `x-singer.decimal` format instead of as `number`.

        Returns:
            A new instance of the class.
        """
        return cls(use_singer_decimal=config.get("use_singer_decimal", False))

    @functools.singledispatchmethod
    def to_jsonschema(self, column_type: sa.types.TypeEngine) -> dict:  # noqa: ARG002, D102, PLR6301
        return th.StringType.type_dict  # type: ignore[no-any-return]

    @to_jsonschema.register
    def datetime_to_jsonschema(self, column_type: sa.types.DateTime) -> dict:  # noqa: ARG002, PLR6301
        """Return a JSON Schema representation of a generic datetime type.

        Args:
            column_type (:column_type:`DateTime`): The column type.
        """
        return th.DateTimeType.type_dict  # type: ignore[no-any-return]

    @to_jsonschema.register
    def date_to_jsonschema(self, column_type: sa.types.Date) -> dict:  # noqa: ARG002, PLR6301
        """Return a JSON Schema representation of a date type.

        Args:
            column_type (:column_type:`Date`): The column type.
        """
        return th.DateType.type_dict  # type: ignore[no-any-return]

    @to_jsonschema.register
    def time_to_jsonschema(self, column_type: sa.types.Time) -> dict:  # noqa: ARG002, PLR6301
        """Return a JSON Schema representation of a time type.

        Args:
            column_type (:column_type:`Time`): The column type.
        """
        return th.TimeType.type_dict  # type: ignore[no-any-return]

    @to_jsonschema.register
    def integer_to_jsonschema(self, column_type: sa.types.Integer) -> dict:  # noqa: ARG002, PLR6301
        """Return a JSON Schema representation of a an integer type.

        Args:
            column_type (:column_type:`Integer`): The column type.
        """
        return th.IntegerType.type_dict  # type: ignore[no-any-return]

    @to_jsonschema.register
    def float_to_jsonschema(self, column_type: sa.types.Numeric) -> dict:  # noqa: ARG002
        """Return a JSON Schema representation of a generic number type.

        Args:
            column_type (:column_type:`Numeric`): The column type.
        """
        if self.use_singer_decimal:
            return th.SingerDecimalType.type_dict  # type: ignore[no-any-return]
        return th.NumberType.type_dict  # type: ignore[no-any-return]

    @to_jsonschema.register
    def string_to_jsonschema(self, column_type: sa.types.String) -> dict:  # noqa: PLR6301
        """Return a JSON Schema representation of a generic string type.

        Args:
            column_type (:column_type:`String`): The column type.

        .. versionchanged:: 0.41.0
           The :column_type:`length <String.params.length>` attribute is now used to
           determine the maximum length of the string type.
        """
        if column_type.length:
            return th.StringType(max_length=column_type.length).type_dict  # type: ignore[no-any-return]
        return th.StringType.type_dict  # type: ignore[no-any-return]

    @to_jsonschema.register
    def boolean_to_jsonschema(self, column_type: sa.types.Boolean) -> dict:  # noqa: ARG002, PLR6301
        """Return a JSON Schema representation of a boolean type.

        Args:
            column_type (:column_type:`Boolean`): The column type.
        """
        return th.BooleanType.type_dict  # type: ignore[no-any-return]


JSONtoSQLHandler: TypeAlias = t.Union[
    type[sa.types.TypeEngine],
    t.Callable[[dict], sa.types.TypeEngine],
]


class JSONSchemaToSQL:
    """A configurable mapper for converting JSON Schema types to SQLAlchemy types.

    This class provides a mapping from JSON Schema types to SQLAlchemy types.

    .. versionadded:: 0.42.0
    .. versionchanged:: 0.44.0
       Added the
       :meth:`singer_sdk.connectors.sql.JSONSchemaToSQL.register_sql_datatype_handler`
       method to map custom ``x-sql-datatype`` annotations into SQLAlchemy types.
    """

    def __init__(self, *, max_varchar_length: int | None = None) -> None:
        """Initialize the mapper with default type mappings.

        Args:
            max_varchar_length: The absolute maximum length for VARCHAR columns that
                the database supports.
        """
        self._max_varchar_length = max_varchar_length

        # Default type mappings
        self._type_mapping: dict[str, JSONtoSQLHandler] = {
            "string": self._handle_string_type,
            "integer": sa.types.INTEGER,
            "number": sa.types.DECIMAL,
            "boolean": sa.types.BOOLEAN,
            "object": sa.types.VARCHAR,
            "array": sa.types.VARCHAR,
        }

        # Format handlers for string types
        self._format_handlers: dict[str, JSONtoSQLHandler] = {
            # Default date-like formats
            "date-time": sa.types.DATETIME,
            "time": sa.types.TIME,
            "date": sa.types.DATE,
            # Common string formats with sensible defaults
            "uuid": sa.types.UUID,
            "email": lambda _: sa.types.VARCHAR(254),  # RFC 5321
            "uri": lambda _: sa.types.VARCHAR(2083),  # Common browser limit
            "hostname": lambda _: sa.types.VARCHAR(253),  # RFC 1035
            "ipv4": lambda _: sa.types.VARCHAR(15),
            "ipv6": lambda _: sa.types.VARCHAR(45),
            "x-singer.decimal": self._handle_singer_decimal,
        }

        self._sql_datatype_mapping: dict[str, JSONtoSQLHandler] = {}

        self._fallback_type: type[sa.types.TypeEngine] = sa.types.VARCHAR

    @classmethod
    def from_config(
        cls: type[JSONSchemaToSQL],
        config: dict,  # noqa: ARG003
        *,
        max_varchar_length: int | None,
    ) -> JSONSchemaToSQL:
        """Create a new instance from a configuration dictionary.

        Override this to instantiate this converter with values from the target's
        configuration dictionary.

        .. code-block:: python

              class CustomJSONSchemaToSQL(JSONSchemaToSQL):
                  @classmethod
                  def from_config(cls, config, **kwargs):
                      return cls(max_varchar_length=config.get("max_varchar_length"))

        Args:
            config: The configuration dictionary.
            max_varchar_length: The absolute maximum length for VARCHAR columns that
                the database supports.

        Returns:
            A new instance of the class.
        """
        return cls(max_varchar_length=max_varchar_length)

    def _invoke_handler(  # noqa: PLR6301
        self,
        handler: JSONtoSQLHandler,
        schema: dict,
    ) -> sa.types.TypeEngine:
        """Invoke a handler, handling both type classes and callables.

        Args:
            handler: The handler to invoke.
            schema: The schema to pass to callable handlers.

        Returns:
            The resulting SQLAlchemy type.
        """
        if isinstance(handler, type):
            return handler()  # type: ignore[no-any-return]
        return handler(schema)

    def _handle_singer_decimal(self, schema: dict) -> sa.types.TypeEngine:  # noqa: PLR6301
        """Handle a x-singer.decimal format.

        Args:
            schema: The JSON Schema object.

        Returns:
            The appropriate SQLAlchemy type.
        """
        return sa.types.DECIMAL(schema.get("precision"), schema.get("scale"))

    @property
    def fallback_type(self) -> type[sa.types.TypeEngine]:
        """Return the fallback type.

        Returns:
            The fallback type.
        """
        return self._fallback_type

    @fallback_type.setter
    def fallback_type(self, value: type[sa.types.TypeEngine]) -> None:
        """Set the fallback type.

        Args:
            value: The new fallback type.
        """
        self._fallback_type = value

    def register_type_handler(self, json_type: str, handler: JSONtoSQLHandler) -> None:
        """Register a custom type handler.

        Args:
            json_type: The JSON Schema type to handle.
            handler: Either a SQLAlchemy type class or a callable that takes a schema
                    dict and returns a SQLAlchemy type instance.
        """
        self._type_mapping[json_type] = handler

    def register_format_handler(
        self,
        format_name: str,
        handler: JSONtoSQLHandler,
    ) -> None:
        """Register a custom format handler.

        Args:
            format_name: The format string (e.g., "date-time", "email", "custom-format").
            handler: Either a SQLAlchemy type class or a callable that takes a schema
                    dict and returns a SQLAlchemy type instance.
        """  # noqa: E501
        self._format_handlers[format_name] = handler

    def register_sql_datatype_handler(
        self,
        sql_datatype: str,
        handler: JSONtoSQLHandler,
    ) -> None:
        """Register a custom ``x-sql-datatype`` handler.

        Args:
            sql_datatype: The x-sql-datatype string.
            handler: Either a SQLAlchemy type class or a callable that takes a schema
                    dict and returns a SQLAlchemy type instance.

        Example:
            >>> from sqlalchemy.types import SMALLINT
            >>> to_sql = JSONSchemaToSQL()
            >>> to_sql.register_sql_datatype_handler("smallint", SMALLINT)
        """
        self._sql_datatype_mapping[sql_datatype] = handler

    def handle_multiple_types(self, types: t.Sequence[str]) -> sa.types.TypeEngine:  # noqa: ARG002, PLR6301
        """Handle multiple types by returning a VARCHAR.

        Args:
            types: The list of types to handle.

        Returns:
            A VARCHAR type.
        """
        return sa.types.VARCHAR()

    def handle_raw_string(self, schema: dict) -> sa.types.TypeEngine:
        """Handle a string type generically.

        Args:
            schema: The JSON Schema object.

        Returns:
            Appropriate SQLAlchemy type.
        """
        max_length: int | None = schema.get("maxLength")

        if max_length and self._max_varchar_length:
            max_length = min(max_length, self._max_varchar_length)

        return sa.types.VARCHAR(max_length)

    def _get_type_from_schema(self, schema: dict) -> sa.types.TypeEngine | None:
        """Try to get a SQL type from a single schema object.

        Args:
            schema: The JSON Schema object.

        Returns:
            SQL type if one can be determined, None otherwise.
        """
        # Check x-sql-datatype first
        if x_sql_datatype := schema.get("x-sql-datatype"):
            if handler := self._sql_datatype_mapping.get(x_sql_datatype):
                return self._invoke_handler(handler, schema)

            warnings.warn(
                f"This target does not support the x-sql-datatype '{x_sql_datatype}'",
                UserWarning,
                stacklevel=2,
            )

        # Check if this is a string with format then
        if schema.get("type") == "string" and "format" in schema:  # noqa: SIM102
            if (format_type := self._handle_format(schema)) is not None:
                return format_type

        # Then check regular types
        if schema_type := schema.get("type"):
            if isinstance(schema_type, (list, tuple)):
                # Filter out null type if present
                non_null_types = [t for t in schema_type if t != "null"]

                # If we have multiple non-null types, use VARCHAR
                if len(non_null_types) > 1:
                    self.handle_multiple_types(non_null_types)

                # If we have exactly one non-null type, use its handler
                if len(non_null_types) == 1 and non_null_types[0] in self._type_mapping:
                    handler = self._type_mapping[non_null_types[0]]
                    return self._invoke_handler(handler, schema)

            elif type_handler := self._type_mapping.get(schema_type):
                return self._invoke_handler(type_handler, schema)

        return None

    def _handle_format(self, schema: dict) -> sa.types.TypeEngine | None:
        """Handle format-specific type conversion.

        Args:
            schema: The JSON Schema object.

        Returns:
            The format-specific SQL type if applicable, None otherwise.
        """
        if "format" not in schema:
            return None

        format_string: str = schema["format"]

        if handler := self._format_handlers.get(format_string):
            return self._invoke_handler(handler, schema)

        return None

    def _handle_string_type(self, schema: dict) -> sa.types.TypeEngine:
        """Handle string type conversion with special cases for formats.

        Args:
            schema: The JSON Schema object.

        Returns:
            Appropriate SQLAlchemy type.
        """
        # Check for format-specific handling first
        if format_type := self._handle_format(schema):
            return format_type

        return self.handle_raw_string(schema)

    def to_sql_type(self, schema: dict) -> sa.types.TypeEngine:
        """Convert a JSON Schema type definition to a SQLAlchemy type.

        Args:
            schema: The JSON Schema object.

        Returns:
            The corresponding SQLAlchemy type.
        """
        if sql_type := self._get_type_from_schema(schema):
            return sql_type

        # Handle anyOf
        if "anyOf" in schema:
            for subschema in schema["anyOf"]:
                # Skip null types in anyOf
                if subschema.get("type") == "null":
                    continue

                if sql_type := self._get_type_from_schema(subschema):
                    return sql_type

        # Fallback
        return self.fallback_type()


class SQLConnector:  # noqa: PLR0904
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

    #: The absolute maximum length for VARCHAR columns that the database supports.
    max_varchar_length: int | None = None

    #: The SQL-to-JSON type mapper class for this SQL connector. Override this property
    #: with a subclass of :class:`~singer_sdk.connectors.sql.SQLToJSONSchema` to provide
    #: a custom mapping for your SQL dialect.
    sql_to_jsonschema_converter: type[SQLToJSONSchema] = SQLToJSONSchema

    #: The JSON-to-SQL type mapper class for this SQL connector. Override this property
    #: with a subclass of :class:`~singer_sdk.connectors.sql.JSONSchemaToSQL` to provide
    #: a custom mapping for your SQL dialect.
    jsonschema_to_sql_converter: type[JSONSchemaToSQL] = JSONSchemaToSQL

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

    @functools.cached_property
    def sql_to_jsonschema(self) -> SQLToJSONSchema:
        """The SQL-to-JSON type mapper object for this SQL connector.

        Override this property to provide a custom mapping for your SQL dialect.

        .. versionadded:: 0.41.0
        """
        return self.sql_to_jsonschema_converter.from_config(self.config)

    @functools.cached_property
    def jsonschema_to_sql(self) -> JSONSchemaToSQL:
        """The JSON-to-SQL type mapper object for this SQL connector.

        Override this property to provide a custom mapping for your SQL dialect.

        .. versionadded:: 0.42.0
        """
        return self.jsonschema_to_sql_converter.from_config(
            self.config,
            max_varchar_length=self.max_varchar_length,
        )

    @contextmanager
    def _connect(self) -> t.Iterator[sa.engine.Connection]:
        with self._engine.connect().execution_options(stream_results=True) as conn:
            yield conn

    @deprecated(
        "`SQLConnector.create_sqlalchemy_connection` is deprecated. "
        "If you need to execute something that isn't available "
        "on the connector currently, make a child class and "
        "add your required method on that connector.",
        category=DeprecationWarning,
        stacklevel=1,
    )
    def create_sqlalchemy_connection(self) -> sa.engine.Connection:
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
        return self._engine.connect().execution_options(stream_results=True)

    @deprecated(
        "`SQLConnector.create_sqlalchemy_engine` is deprecated. Override "
        "`_engine` or `sqlalchemy_url` instead.",
        category=DeprecationWarning,
        stacklevel=1,
    )
    def create_sqlalchemy_engine(self) -> Engine:
        """(DEPRECATED) Return a new SQLAlchemy engine using the provided config.

        Developers can generally override just one of the following:
        `sqlalchemy_engine`, sqlalchemy_url`.

        Returns:
            A newly created SQLAlchemy engine object.
        """
        return self._engine

    @property
    def connection(self) -> sa.engine.Connection:
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

    def get_sqlalchemy_url(self, config: dict[str, t.Any]) -> str:  # noqa: PLR6301
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

        return t.cast("str", config["sqlalchemy_url"])

    def to_jsonschema_type(
        self,
        sql_type: (
            str  # noqa: ANN401
            | sa.types.TypeEngine
            | type[sa.types.TypeEngine]
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

        .. versionchanged:: 0.40.0
           Support for SQLAlchemy type classes and strings in the ``sql_type`` argument
           was deprecated. Please pass a SQLAlchemy type object instead.
        """
        if isinstance(sql_type, sa.types.TypeEngine):
            return self.sql_to_jsonschema.to_jsonschema(sql_type)

        if isinstance(sql_type, str):  # pragma: no cover
            warnings.warn(
                "Passing string types to `to_jsonschema_type` is deprecated. "
                "Please pass a SQLAlchemy type object instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            return th.to_jsonschema_type(sql_type)

        if isinstance(sql_type, type):  # pragma: no cover
            warnings.warn(
                "Passing type classes to `to_jsonschema_type` is deprecated. "
                "Please pass a SQLAlchemy type object instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            if issubclass(sql_type, sa.types.TypeEngine):
                return th.to_jsonschema_type(sql_type)

            msg = f"Unexpected type received: '{sql_type.__name__}'"
            raise ValueError(msg)

        msg = f"Unexpected type received: '{type(sql_type).__name__}'"
        raise ValueError(msg)

    def to_sql_type(self, jsonschema_type: dict) -> sa.types.TypeEngine:
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
        return self.jsonschema_to_sql.to_sql_type(jsonschema_type)

    @staticmethod
    def get_fully_qualified_name(
        table_name: str | None = None,
        schema_name: str | None = None,
        db_name: str | None = None,
        delimiter: str = ".",
    ) -> FullyQualifiedName:
        """Concatenates a fully qualified name from the parts.

        Args:
            table_name: The name of the table.
            schema_name: The name of the schema. Defaults to None.
            db_name: The name of the database. Defaults to None.
            delimiter: Generally: '.' for SQL names and '-' for Singer names.

        Returns:
            The fully qualified name as a string.

        .. versionchanged:: 0.40.0
           A ``FullyQualifiedName`` object is now returned.
        """
        return FullyQualifiedName(
            table=table_name,  # type: ignore[arg-type]
            schema=schema_name,
            database=db_name,
            delimiter=delimiter,
        )

    @property
    def _dialect(self) -> sa.engine.Dialect:
        """Return the dialect object.

        Returns:
            The dialect object.
        """
        return self._engine.dialect

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
        return self._cached_engine

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
            return sa.create_engine(
                self.sqlalchemy_url,
                echo=False,
                pool_pre_ping=True,
                json_serializer=self.serialize_json,
                json_deserializer=self.deserialize_json,
            )
        except TypeError:
            self.logger.exception(
                "Retrying engine creation with fewer arguments due to TypeError.",
            )
            return sa.create_engine(
                self.sqlalchemy_url,
                echo=False,
                pool_pre_ping=True,
            )

    @deprecated(
        "This method is deprecated. Use or override `FullyQualifiedName` instead.",
        category=SingerSDKDeprecationWarning,
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
        return ".".join(  # pragma: no cover
            [
                self._dialect.identifier_preparer.quote(name_part)
                for name_part in name.split(".")
            ],
        )

    @lru_cache  # noqa: B019
    def _warn_no_view_detection(self) -> None:
        """Print a warning, but only the first time."""
        self.logger.warning(
            "Provider does not support get_view_names(). "
            "Streams list may be incomplete or `is_view` may be unpopulated.",
        )

    def get_schema_names(  # noqa: PLR6301
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

    @deprecated(
        "This method is deprecated.",
        category=SingerSDKDeprecationWarning,
        stacklevel=1,
    )
    def get_object_names(  # pragma: no cover
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

    # TODO maybe should be split into smaller parts?
    def discover_catalog_entry(
        self,
        engine: Engine,  # noqa: ARG002
        inspected: Inspector,  # noqa: ARG002
        schema_name: str | None,
        table_name: str,
        is_view: bool,  # noqa: FBT001
        *,
        reflected_columns: list[reflection.ReflectedColumn] | None = None,
        reflected_pk: reflection.ReflectedPrimaryKeyConstraint | None = None,
        reflected_indices: list[reflection.ReflectedIndex] | None = None,
    ) -> CatalogEntry:
        """Create `CatalogEntry` object for the given table or a view.

        Args:
            engine: SQLAlchemy engine
            inspected: SQLAlchemy inspector instance for engine
            schema_name: Schema name to inspect
            table_name: Name of the table or a view
            is_view: Flag whether this object is a view, returned by `get_object_names`
            reflect_indices: Whether to reflect indices
            reflected_columns: List of reflected columns
            reflected_pk: Reflected primary key
            reflected_indices: List of reflected indices

        Returns:
            `CatalogEntry` object for the given table or a view
        """
        # Initialize unique stream name
        unique_stream_id = f"{schema_name}-{table_name}" if schema_name else table_name

        # Backwards-compatibility
        reflected_columns = reflected_columns or []
        reflected_indices = reflected_indices or []

        # Detect key properties
        possible_primary_keys: list[list[str]] = []
        if reflected_pk and "constrained_columns" in reflected_pk:
            possible_primary_keys.append(reflected_pk["constrained_columns"])

        # An element of the columns list is ``None`` if it's an expression and is
        # returned in the ``expressions`` list of the reflected index.
        possible_primary_keys.extend(
            index_def["column_names"]  # type: ignore[misc]
            for index_def in reflected_indices
            if index_def.get("unique", False)
        )

        key_properties = next(iter(possible_primary_keys), [])

        # Initialize columns list
        properties = [
            th.Property(
                name=column["name"],
                wrapped=th.CustomType(self.to_jsonschema_type(column["type"])),
                nullable=column.get("nullable", False),
                required=column["name"] in key_properties,
                description=column.get("comment"),
            )
            for column in reflected_columns
        ]
        schema = th.PropertiesList(*properties).to_dict()

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

    def discover_catalog_entries(
        self,
        *,
        exclude_schemas: t.Sequence[str] = (),
        reflect_indices: bool = True,
    ) -> list[dict]:
        """Return a list of catalog entries from discovery.

        Args:
            exclude_schemas: A list of schema names to exclude from discovery.
            reflect_indices: Whether to reflect indices to detect potential primary
                keys.

        Returns:
            The discovered catalog entries as a list.
        """
        result: list[dict] = []
        engine = self._engine
        inspected = sa.inspect(engine)
        object_kinds = (
            (reflection.ObjectKind.TABLE, False),
            (reflection.ObjectKind.ANY_VIEW, True),
        )
        for schema_name in self.get_schema_names(engine, inspected):
            if schema_name in exclude_schemas:
                continue

            primary_keys = inspected.get_multi_pk_constraint(schema=schema_name)

            if reflect_indices:
                indices = inspected.get_multi_indexes(schema=schema_name)
            else:
                indices = {}

            for object_kind, is_view in object_kinds:
                columns = inspected.get_multi_columns(
                    schema=schema_name,
                    kind=object_kind,
                )

                result.extend(
                    self.discover_catalog_entry(
                        engine,
                        inspected,
                        schema_name,
                        table,
                        is_view,
                        reflected_columns=columns[schema, table],
                        reflected_pk=primary_keys.get((schema, table)),
                        reflected_indices=indices.get((schema, table), []),
                    ).to_dict()
                    for schema, table in columns
                )

        return result

    def parse_full_table_name(  # noqa: PLR6301
        self,
        full_table_name: str | FullyQualifiedName,
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
        if isinstance(full_table_name, FullyQualifiedName):
            return (
                full_table_name.database,
                full_table_name.schema,
                full_table_name.table,
            )

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

    def table_exists(self, full_table_name: str | FullyQualifiedName) -> bool:
        """Determine if the target table already exists.

        Args:
            full_table_name: the target table name.

        Returns:
            True if table exists, False if not, None if unsure or undetectable.
        """
        _, schema_name, table_name = self.parse_full_table_name(full_table_name)

        return sa.inspect(self._engine).has_table(table_name, schema_name)

    def schema_exists(self, schema_name: str) -> bool:
        """Determine if the target database schema already exists.

        Args:
            schema_name: The target database schema name.

        Returns:
            True if the database schema exists, False if not.
        """
        schemas = set(sa.inspect(self._engine).get_schema_names())
        return schema_name in schemas

    def get_table_columns(
        self,
        full_table_name: str | FullyQualifiedName,
        column_names: list[str] | None = None,
    ) -> dict[str, sa.Column]:
        """Return a list of table columns.

        Args:
            full_table_name: Fully qualified table name.
            column_names: A list of column names to filter to.

        Returns:
            An ordered list of column objects.
        """
        _, schema_name, table_name = self.parse_full_table_name(full_table_name)
        inspector = sa.inspect(self._engine)
        columns = inspector.get_columns(table_name, schema_name)

        columns_dict: dict[str, sa.Column] = {
            col_meta["name"]: sa.Column(
                col_meta["name"],
                col_meta["type"],
                nullable=col_meta.get("nullable", False),
            )
            for col_meta in columns
            if not column_names
            or col_meta["name"].casefold() in {col.casefold() for col in column_names}
        }

        return columns_dict

    def get_table(
        self,
        full_table_name: str | FullyQualifiedName,
        column_names: list[str] | None = None,
    ) -> sa.Table:
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
        meta = sa.MetaData()
        return sa.schema.Table(
            table_name,
            meta,
            *list(columns),
            schema=schema_name,
        )

    def column_exists(
        self, full_table_name: str | FullyQualifiedName, column_name: str
    ) -> bool:
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
            conn.execute(sa.schema.CreateSchema(schema_name))

    def create_empty_table(
        self,
        full_table_name: str | FullyQualifiedName,
        schema: dict,
        primary_keys: t.Sequence[str] | None = None,
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
        meta = sa.MetaData(schema=schema_name)
        columns: list[sa.Column] = []
        primary_keys = primary_keys or []
        try:
            properties: dict = schema["properties"]
        except KeyError as e:
            msg = f"Schema for '{full_table_name}' does not define properties: {schema}"
            raise RuntimeError(msg) from e
        for property_name, property_jsonschema in properties.items():
            is_primary_key = property_name in primary_keys
            columns.append(
                sa.Column(
                    property_name,
                    self.to_sql_type(property_jsonschema),
                    primary_key=is_primary_key,
                ),
            )

        _ = sa.Table(table_name, meta, *columns)
        meta.create_all(self._engine)

    def _create_empty_column(
        self,
        full_table_name: str | FullyQualifiedName,
        column_name: str,
        sql_type: sa.types.TypeEngine,
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
        if not self.schema_exists(schema_name):
            self.create_schema(schema_name)

    def prepare_table(
        self,
        full_table_name: str | FullyQualifiedName,
        schema: dict,
        primary_keys: t.Sequence[str],
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

        self.prepare_primary_key(
            full_table_name=full_table_name,
            primary_keys=primary_keys,
        )

    def prepare_primary_key(
        self,
        *,
        full_table_name: str | FullyQualifiedName,  # noqa: ARG002
        primary_keys: t.Sequence[str],  # noqa: ARG002
    ) -> None:
        """Adapt target table primary key to provided schema if possible.

        Implement this method in a subclass to adapt the primary key of the target table
        to the provided one if possible.

        Args:
            full_table_name: the target table name.
            primary_keys: list of key properties.
        """
        self.logger.debug("Primary key adaptation is not implemented")

    def prepare_column(
        self,
        full_table_name: str | FullyQualifiedName,
        column_name: str,
        sql_type: sa.types.TypeEngine,
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

    def rename_column(
        self, full_table_name: str | FullyQualifiedName, old_name: str, new_name: str
    ) -> None:
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
        sql_types: t.Sequence[sa.types.TypeEngine],
    ) -> sa.types.TypeEngine:
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
                [self.merge_sql_types([sql_types[0], sql_types[1]]), *sql_types[2:]],
            )

        # Get the generic type class
        for opt in sql_types:
            # Get the length
            opt_len: int | None = getattr(opt, "length", 0)
            generic_type = type(opt.as_generic())

            if isinstance(generic_type, type):
                if issubclass(
                    generic_type,
                    (sa.types.String, sa.types.Unicode),
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

    def _sort_types(  # noqa: PLR6301
        self,
        sql_types: t.Iterable[sa.types.TypeEngine],
    ) -> t.Sequence[sa.types.TypeEngine]:
        """Return the input types sorted from most to least compatible.

        For example, [Smallint, Integer, Datetime, String, Double] would become
        [Unicode, String, Double, Integer, Smallint, Datetime].

        String types will be listed first, then decimal types, then integer types,
        then bool types, and finally datetime and date. Higher precision, scale, and
        length will be sorted earlier.

        Args:
            sql_types (List[sa.types.TypeEngine]): [description]

        Returns:
            The sorted list.
        """

        def _get_type_sort_key(
            sql_type: sa.types.TypeEngine,
        ) -> tuple[int, int]:
            # return rank, with higher numbers ranking first

            len_ = int(getattr(sql_type, "length", 0) or 0)

            pytype = t.cast("type", sql_type.python_type)
            if issubclass(pytype, (str, bytes)):
                return 900, len_
            if issubclass(pytype, datetime):
                return 600, len_
            if issubclass(pytype, float):
                return 400, len_
            if issubclass(pytype, int):
                return 300, len_

            return 0, len_

        return sorted(sql_types, key=_get_type_sort_key, reverse=True)

    def _get_column_type(
        self,
        full_table_name: str | FullyQualifiedName,
        column_name: str,
    ) -> sa.types.TypeEngine:
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

        return column.type

    def get_column_add_ddl(
        self,
        table_name: str | FullyQualifiedName,
        column_name: str,
        column_type: sa.types.TypeEngine,
    ) -> sa.DDL:
        """Get the create column DDL statement.

        Override this if your database uses a different syntax for creating columns.

        Args:
            table_name: Fully qualified table name of column to alter.
            column_name: Column name to create.
            column_type: New column sqlalchemy type.

        Returns:
            A sqlalchemy DDL instance.
        """
        create_column_clause = sa.schema.CreateColumn(
            sa.Column(
                column_name,
                column_type,
            ),
        )
        compiled = create_column_clause.compile(self._engine).string
        return sa.DDL(
            "ALTER TABLE %(table_name)s ADD COLUMN %(create_column_clause)s",
            {
                "table_name": table_name,
                "create_column_clause": compiled,
            },
        )

    @staticmethod
    def get_column_rename_ddl(
        table_name: str | FullyQualifiedName,
        column_name: str,
        new_column_name: str,
    ) -> sa.DDL:
        """Get the create column DDL statement.

        Override this if your database uses a different syntax for renaming columns.

        Args:
            table_name: Fully qualified table name of column to alter.
            column_name: Existing column name.
            new_column_name: New column name.

        Returns:
            A sqlalchemy DDL instance.
        """
        return sa.DDL(
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
        table_name: str | FullyQualifiedName,
        column_name: str,
        column_type: sa.types.TypeEngine,
    ) -> sa.DDL:
        """Get the alter column DDL statement.

        Override this if your database uses a different syntax for altering columns.

        Args:
            table_name: Fully qualified table name of column to alter.
            column_name: Column name to alter.
            column_type: New column type string.

        Returns:
            A sqlalchemy DDL instance.
        """
        return sa.DDL(
            "ALTER TABLE %(table_name)s ALTER COLUMN %(column_name)s (%(column_type)s)",
            {
                "table_name": table_name,
                "column_name": column_name,
                "column_type": column_type,
            },
        )

    @staticmethod
    def remove_collation(
        column_type: sa.types.TypeEngine,
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
        column_type: sa.types.TypeEngine,
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
        full_table_name: str | FullyQualifiedName,
        column_name: str,
        sql_type: sa.types.TypeEngine,
    ) -> None:
        """Adapt table column type to support the new JSON schema type.

        Args:
            full_table_name: The target table name.
            column_name: The target column name.
            sql_type: The new SQLAlchemy type.

        Raises:
            NotImplementedError: if altering columns is not supported.
        """
        current_type: sa.types.TypeEngine = self._get_column_type(
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

    def serialize_json(self, obj: object) -> str:  # noqa: PLR6301
        """Serialize an object to a JSON string.

        Target connectors may override this method to provide custom serialization logic
        for JSON types.

        Args:
            obj: The object to serialize.

        Returns:
            The JSON string.

        .. versionadded:: 0.31.0
        """
        return dump_json(obj)

    def deserialize_json(self, json_str: str) -> object:  # noqa: PLR6301
        """Deserialize a JSON string to an object.

        Tap connectors may override this method to provide custom deserialization
        logic for JSON types.

        Args:
            json_str: The JSON string to deserialize.

        Returns:
            The deserialized object.

        .. versionadded:: 0.31.0
        """
        return load_json(json_str)

    def delete_old_versions(
        self,
        *,
        full_table_name: str | FullyQualifiedName,
        version_column_name: str,
        current_version: int,
    ) -> None:
        """Hard-deletes any old version rows from the table.

        This is used to clean up old versions when an ACTIVATE_VERSION message is
        received.

        Args:
            full_table_name: The fully qualified table name.
            version_column_name: The name of the version column.
            current_version: The current ACTIVATE version of the table.
        """
        with self._connect() as conn, conn.begin():
            conn.execute(
                sa.text(
                    f"DELETE FROM {full_table_name} "  # noqa: S608
                    f"WHERE {version_column_name} < {current_version}",
                ),
            )
