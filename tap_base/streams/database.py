"""Base class for database-type streams."""

import abc
import backoff

import singer
from tap_base.helpers import classproperty
from tap_base.exceptions import TapStreamConnectionFailure
from typing import Any, Dict, Iterable, List, Optional, Tuple, TypeVar, Union, cast

from tap_base.streams.core import Stream

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

    @property
    def records(self) -> Iterable[dict]:
        """Return a generator of row-type dictionary objects."""
        for row in self.sql_query(
            sql=f"SELECT * FROM {self.fully_qualified_name}", config=self.config
        ):
            yield cast(dict, row)

    @property
    def fully_qualified_name(self):
        return self.tap_stream_id

    @classproperty
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
        props: Dict[str, singer.Schema] = {}
        for column, sql_type in columns.items():
            props[column] = DatabaseStream.get_singer_type(sql_type)
        return singer.Schema(type="object", properties=props)

    @staticmethod
    def get_singer_type(sql_type: str) -> singer.Schema:
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
        columns_scan_result = cls.sql_query(config=config, sql=cls.column_scan_sql)
        result: Dict[Tuple[str, str, str], Dict[str, str]] = {}
        for row_dict in columns_scan_result:
            catalog, schema_name, table, column, data_type = row_dict.values()
            if (catalog, schema_name, table) not in result:
                result[(catalog, schema_name, table)] = {}
            result[(catalog, schema_name, table)][column] = data_type
        return result

    @classmethod
    def scan_primary_keys(cls, config) -> Dict[Tuple[str, str, str], List[str]]:
        result: Dict[Tuple[str, str, str], List[str]] = {}
        if not cls.primary_key_scan_sql:
            return result
        pk_scan_result = cls.sql_query(config=config, sql=cls.primary_key_scan_sql)
        for row_dict in pk_scan_result:
            catalog, schema_name, table, pk_column = row_dict.values()
            if (catalog, schema_name, table) not in result:
                result[(catalog, schema_name, table)] = []
            result[(catalog, schema_name, table)].append(pk_column)
        return result

    @classmethod
    def from_discovery(cls, config: dict) -> List[FactoryType]:
        """Return a list of all streams (tables)."""
        result: List[DatabaseStream] = []
        table_scan_result: Iterable[List[Any]] = cls.sql_query(
            config=config, sql=cls.table_scan_sql, dict_results=False
        )
        view_scan_result: Iterable[List[Any]] = cls.sql_query(
            config=config, sql=cls.view_scan_sql, dict_results=False
        )
        all_results = [
            (database, schema_name, table, False)
            for database, schema_name, table in table_scan_result
        ] + [
            (database, schema_name, table, True)
            for database, schema_name, table in view_scan_result
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
            new_stream = cls(
                config=config, schema=singer_schema.to_dict(), name=full_name, state={},
            )
            new_stream.primary_keys = primary_keys
            # TODO: Expanded metadata support for setting `row_count` and `is_view`.
            # new_stream.is_view = is_view
            # new_stream.row_count = row_count
            result.append(new_stream)
        return result

    @abc.abstractclassmethod
    def sql_query(
        self, sql: Union[str, List[str]], config, dict_results=True
    ) -> Union[Iterable[dict], Iterable[Tuple]]:
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
        # self.logger.info(
        print(
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

    @abc.abstractclassmethod
    def open_connection(cls, config) -> Any:
        """Connect to the database source."""
        pass
