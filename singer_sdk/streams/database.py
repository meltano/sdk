"""Base class for database-type streams."""

import abc
from pathlib import Path
import backoff

import singer
from singer.schema import Schema
from singer_sdk.helpers.util import (
    get_catalog_entries,
    get_catalog_entry_name,
    get_catalog_entry_schema,
)
from singer_sdk.helpers.classproperty import classproperty
from singer_sdk.helpers.util import read_json_file
from singer_sdk.exceptions import TapStreamConnectionFailure
from typing import Any, Dict, Iterable, List, Optional, Tuple, TypeVar, Union, cast

from singer_sdk.plugin_base import PluginBase as TapBaseClass
from singer_sdk.streams.core import Stream

FactoryType = TypeVar("FactoryType", bound="DatabaseStream")


SINGER_STRING_TYPE = singer.Schema(type=["string", "null"])
SINGER_FLOAT_TYPE = singer.Schema(type=["double", "null"])
SINGER_INT_TYPE = singer.Schema(type=["int", "null"])
SINGER_DECIMAL_TYPE = singer.Schema(type=["decimal", "null"])
SINGER_DATETIME_TYPE = singer.Schema(type=["string", "null"], format="date-time")
SINGER_BOOLEAN_TYPE = singer.Schema(type=["boolean", "null"])
SINGER_OBJECT_TYPE = singer.Schema(type=["string", "object", "null"])
SINGER_TYPE_LOOKUP = {
    # NOTE: This is an ordered mapping, with earlier mappings taking precedence.
    #       If the SQL-provided type contains the type name on the left, the mapping
    #       will return the respective singer type.
    "timestamp": SINGER_DATETIME_TYPE,
    "datetime": SINGER_DATETIME_TYPE,
    "date": SINGER_DATETIME_TYPE,
    "int": SINGER_INT_TYPE,
    "number": SINGER_DECIMAL_TYPE,
    "decimal": SINGER_DECIMAL_TYPE,
    "double": SINGER_FLOAT_TYPE,
    "float": SINGER_FLOAT_TYPE,
    "string": SINGER_STRING_TYPE,
    "text": SINGER_STRING_TYPE,
    "char": SINGER_STRING_TYPE,
    "bool": SINGER_BOOLEAN_TYPE,
    "variant": SINGER_STRING_TYPE,  # TODO: Support nested objects.
}


class DatabaseStream(Stream, metaclass=abc.ABCMeta):
    """Abstract base class for database-type streams.

    This class currently supports databases with 3-part names only. For databases which
    use two-part names, further modification to certain methods may be necessary.
    """

    MAX_CONNECT_RETRIES = 5
    THREE_PART_NAMES = True  # For backwards compatibility reasons

    DEFAULT_QUOTE_CHAR = '"'
    OTHER_QUOTE_CHARS = ['"', "[", "]", "`"]

    def __init__(
        self,
        tap: TapBaseClass,
        schema: Optional[Union[str, Path, Dict[str, Any], Schema]],
        name: str,
    ):
        """Initialize the database stream.

        Parameters
        ----------
        tap : TapBaseClass
            reference to the parent tap
        schema : Optional[Union[str, Path, Dict[str, Any], Schema]]
            A schema dict or the path to a valid schema file in json.
        name : str
            Required. Name of the stream (generally the same as the table name).
        """
        super().__init__(tap=tap, schema=schema, name=name)
        self.is_view: Optional[bool] = None
        self.row_count: Optional[int] = None

    def get_records(self, partition: Optional[dict]) -> Iterable[Dict[str, Any]]:
        """Return a generator of row-type dictionary objects.

        Each row emitted should be a dictionary of property names to their values.
        """
        if partition:
            raise NotImplementedError(
                f"Stream '{self.name}' does not support partitioning."
            )
        for row in self.execute_query(
            sql=f"SELECT * FROM {self.fully_qualified_name}", config=self.config
        ):
            yield row

    @property
    def fully_qualified_name(self):
        """Return the fully qualified name of the table name."""
        return self.tap_stream_id

    @classproperty
    # @classmethod
    def table_scan_sql(cls) -> str:
        """Return a SQL statement for syncable tables.

        Result fields should be in this order:
         - db_name
         - schema_name
         - table_name
        """
        return """
            SELECT table_catalog, table_schema, table_name
            from information_schema.tables
            WHERE UPPER(table_type) not like '%VIEW%'
            """

    @classproperty
    # @classmethod
    def view_scan_sql(cls) -> str:
        """Return a SQL statement for syncable views.

        Result fields should be in this order:
         - db_name
         - schema_name
         - view_name
        """
        return """
            SELECT table_catalog, table_schema, table_name
            FROM information_schema.views
            where upper(table_schema) <> 'INFORMATION_SCHEMA'
            """

    @classproperty
    # @classmethod
    def column_scan_sql(cls) -> str:
        """Return a SQL statement that provides the column names and types.

        Result fields should be in this order:
         - db_name
         - schema_name
         - table_name
         - column_name
         - column_type

        Optionally, results can be sorted to preserve cardinal ordinaling.
        """
        return """
            SELECT table_catalog, table_schema, table_name, column_name, data_type
            FROM information_schema.columns
            ORDER BY table_catalog, table_schema, table_name, ordinal_position
            """

    @classproperty
    # @classmethod
    def primary_key_scan_sql(cls) -> Optional[str]:
        """Return a SQL statement that provides the list of primary key columns.

        Result fields should be in this order:
         - db_name
         - schema_name
         - table_name
         - column_name
        """
        return """
            SELECT cols.table_catalog,
                   cols.table_schema,
                   cols.table_name,
                   cols.column_name as key_column
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS constraint
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE cols
                 on cols.constraint_name   = constraint.constraint_name
                and cols.constraint_schema = constraint.constraint_schema
                and cols.constraint_name   = constraint.constraint_name
            WHERE constraint.constraint_type = 'PRIMARY KEY'
            ORDER BY cols.table_schema,
                     cols.table_name,
                     cols.ordinal_position;
            """

    @staticmethod
    def create_singer_schema(columns: Dict[str, str]) -> singer.Schema:
        """Return a singer 'Schema' object with the specified columns and data types."""
        props: Dict[str, singer.Schema] = {}
        for column, sql_type in columns.items():
            props[column] = DatabaseStream.get_singer_type(sql_type)
        return singer.Schema(type="object", properties=props)

    @staticmethod
    def get_singer_type(sql_type: str) -> singer.Schema:
        """Return a singer type class based on the provided sql-base data type."""
        for matchable in SINGER_TYPE_LOOKUP.keys():
            if matchable.lower() in sql_type.lower():
                return SINGER_TYPE_LOOKUP[matchable]
        raise RuntimeError(
            f"Could not infer a Singer data type from type '{sql_type}'."
        )

    @classmethod
    def scan_and_collate_columns(
        cls, config
    ) -> Dict[Tuple[str, str, str], Dict[str, str]]:
        """Return a mapping of columns and datatypes for each table and view."""
        columns_scan_result = cls.execute_query(config=config, sql=cls.column_scan_sql)
        result: Dict[Tuple[str, str, str], Dict[str, str]] = {}
        for row_dict in columns_scan_result:
            row_dict = cast(dict, row_dict)
            catalog, schema_name, table, column, data_type = row_dict.values()
            if (catalog, schema_name, table) not in result:
                result[(catalog, schema_name, table)] = {}
            result[(catalog, schema_name, table)][column] = data_type
        return result

    @classmethod
    def scan_primary_keys(cls, config) -> Dict[Tuple[str, str, str], List[str]]:
        """Return a listing of primary keys for each table and view."""
        result: Dict[Tuple[str, str, str], List[str]] = {}
        if not cls.primary_key_scan_sql:
            return result
        pk_scan_result = cls.execute_query(config=config, sql=cls.primary_key_scan_sql)
        for row_dict in pk_scan_result:
            row_dict = cast(dict, row_dict)
            catalog, schema_name, table, pk_column = row_dict.values()
            if (catalog, schema_name, table) not in result:
                result[(catalog, schema_name, table)] = []
            result[(catalog, schema_name, table)].append(pk_column)
        return result

    @classmethod
    def from_discovery(cls, tap: TapBaseClass) -> List[FactoryType]:
        """Return a list of all streams (tables)."""
        result: List[FactoryType] = []
        config = tap.config
        table_scan_result = cls.execute_query(config=config, sql=cls.table_scan_sql)
        view_scan_result = cls.execute_query(config=config, sql=cls.view_scan_sql)
        all_results = [
            (database, schema_name, table, False)
            for database, schema_name, table in table_scan_result.values()
        ] + [
            (database, schema_name, table, True)
            for database, schema_name, table in view_scan_result.values()
        ]
        collated_columns = cls.scan_and_collate_columns(config=config)
        primary_keys_lookup = cls.scan_primary_keys(config=config)
        for database, schema_name, table, is_view in all_results:
            name_tuple = (database, schema_name, table)
            full_name = ".".join(name_tuple)
            columns = collated_columns.get(name_tuple, None)
            if not columns:
                raise RuntimeError(f"Did not find any columns for table '{full_name}'")
            singer_schema: singer.Schema = cls.create_singer_schema(columns)
            primary_keys = primary_keys_lookup.get(name_tuple, None)
            new_stream = cast(
                FactoryType,
                cls(tap=tap, schema=singer_schema.to_dict(), name=full_name),
            )
            new_stream.primary_keys = primary_keys
            new_stream.is_view = is_view
            result.append(new_stream)
        return result

    @classmethod
    def from_input_catalog(cls, tap: TapBaseClass) -> List[FactoryType]:
        """Initialize streams from an existing catalog, returning a list of streams."""
        result: List[FactoryType] = []
        catalog = tap.input_catalog
        if not catalog:
            raise ValueError(
                "Could not initialize stream from blank or missing catalog."
            )
        for catalog_entry in get_catalog_entries(catalog):
            full_name = get_catalog_entry_name(catalog_entry)
            new_stream = cast(
                FactoryType,
                cls(
                    tap=tap,
                    name=full_name,
                    schema=get_catalog_entry_schema(catalog_entry),
                ),
            )
            result.append(new_stream)
        return result

    # @abc.abstractclassmethod
    @classmethod
    def execute_query(cls, sql: Union[str, List[str]], config) -> Iterable[dict]:
        """Run a SQL query and generate a dict for each returned row."""
        pass

    @classmethod
    def enquote(cls, identifier: str):
        """Escape identifier to be SQL safe."""
        for quotechar in [cls.DEFAULT_QUOTE_CHAR] + cls.OTHER_QUOTE_CHARS:
            if quotechar in identifier:
                raise Exception(
                    f"Can't escape identifier `{identifier}` because it contains a "
                    f"quote character ({quotechar})."
                )
        return f"{cls.DEFAULT_QUOTE_CHAR}{identifier.upper()}{cls.DEFAULT_QUOTE_CHAR}"

    @classmethod
    def dequote(cls, identifier: str):
        """Dequote identifier from quoted version."""
        for quotechar in [cls.DEFAULT_QUOTE_CHAR] + cls.OTHER_QUOTE_CHARS:
            if identifier.startswith(quotechar):
                return identifier.lstrip(quotechar).rstrip(quotechar)

    @classmethod
    def log_backoff_attempt(cls, details):
        """Log backoff attempts used by stream retry_pattern()."""
        self.logger.info(
            "Error communicating with source, "
            f"triggering backoff: {details.get('tries')} try"
        )

    @classmethod
    def connect_with_retries(cls) -> Any:
        """Run open_stream_connection(), retry automatically a few times if failed."""
        return backoff.on_exception(
            backoff.expo,
            exception=TapStreamConnectionFailure,
            max_tries=cls.MAX_CONNECT_RETRIES,
            on_backoff=cls.log_backoff_attempt,
            factor=2,
        )(cls.open_connection)()

    # @abc.abstractclassmethod
    @classmethod
    def open_connection(cls, config) -> Any:
        """Connect to the database source."""
        pass
