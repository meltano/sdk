"""Shared parent class for TapBase, TargetBase, and TransformBase."""

import abc
import backoff

import singer
from tap_base.helpers import classproperty
from tap_base.exceptions import TapStreamConnectionFailure, TooManyRecordsException
from typing import Any, Dict, Iterable, List, Tuple, TypeVar, Union, cast

from tap_base.streams.core import TapStreamBase

FactoryType = TypeVar("FactoryType", bound="DatabaseStreamBase")


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
    "variant": SINGER_STRING_TYPE,  ## TODO: Support nested objects.
}


class DatabaseStreamBase(TapStreamBase, metaclass=abc.ABCMeta):
    """Abstract base class for database-type streams.

    This class currently supports databases with 3-part names only. For databases which
    use two-part names, further modification to certain methods may be necessary.
    """

    MAX_CONNECT_RETRIES = 5
    THREE_PART_NAMES = True  # For backwards compatibility reasons

    DEFAULT_QUOTE_CHAR = '"'
    OTHER_QUOTE_CHARS = ['"', "[", "]", "`"]

    def get_row_generator(self) -> Iterable[dict]:
        """Return a generator of row-type dictionary objects."""
        for row in self.sql_query(
            sql=f"SELECT * FROM {self.fully_qualified_name}", config=self._config
        ):
            yield cast(dict, row)

    @property
    def fully_qualified_name(self):
        return self.tap_stream_id

    @classproperty
    def table_scan_sql(cls) -> str:
        """Return a SQL statement that provides the column."""
        return """
            SELECT table_catalog, table_schema, table_name
            from information_schema.tables
            WHERE UPPER(table_type) not like '%VIEW%'
            """

    @classproperty
    def view_scan_sql(cls) -> str:
        """Return a SQL statement that provides the column."""
        return """
            SELECT table_catalog, table_schema, table_name
            FROM information_schema.views
            where upper(table_schema) <> 'INFORMATION_SCHEMA'
            """

    @classproperty
    def column_scan_sql(cls) -> str:
        """Return a SQL statement that provides the column."""
        return """
            SELECT table_catalog, table_schema, table_name, column_name, data_type
            FROM information_schema.columns
            ORDER BY table_catalog, table_schema, table_name, ordinal_position
            """

    @staticmethod
    def create_singer_schema(columns: Dict[str, str]) -> singer.Schema:
        props: Dict[str, singer.Schema] = {}
        for column, sql_type in columns.items():
            props[column] = DatabaseStreamBase.get_singer_type(sql_type)
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
        columns_scan_result = cls.sql_query(config=config, sql=cls.column_scan_sql)
        result: Dict[Tuple[str, str, str], Dict[str, str]] = {}
        for row_dict in columns_scan_result:
            catalog, schema_name, table, column, data_type = row_dict.values()
            if (catalog, schema_name, table) not in result:
                result[(catalog, schema_name, table)] = {}
            result[(catalog, schema_name, table)][column] = data_type
        return result

    @classmethod
    def from_discovery(cls, config: dict) -> List[FactoryType]:
        """Return a list of all streams (tables)."""
        result: List[DatabaseStreamBase] = []
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
        self, config, sql: Union[str, List[str]], dict_results=True
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
