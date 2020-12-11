"""Shared parent class for TapBase, TargetBase, and TransformBase."""

import abc

import singer
from tap_base.helpers import classproperty
from typing import Dict, Iterable, List, Tuple, TypeVar

from tap_base.streams.core import TapStreamBase

FactoryType = TypeVar("FactoryType", bound="DatabaseStreamBase")


DEFAULT_QUOTE_CHAR = '"'
OTHER_QUOTE_CHARS = ['"', "[", "]", "`"]

SINGER_STRING_TYPE = singer.Schema(type=["string", "null"])
SINGER_FLOAT_TYPE = singer.Schema(type=["double", "null"])
SINGER_INT_TYPE = singer.Schema(type=["int", "null"])
SINGER_DECIMAL_TYPE = singer.Schema(type=["decimal", "null"])
SINGER_DATETIME_TYPE = singer.Schema(type=["string", "null"], format="date-time")
SINGER_TYPE_LOOKUP = {
    # NOTE: This is an ordered mapping, with earlier mappings taking precedence.
    #       If the SQL-provided type contains the type name on the left, the mapping
    #       will return the respective singer type.
    "timestamp": SINGER_DATETIME_TYPE,
    "datetime": SINGER_DATETIME_TYPE,
    "date": SINGER_DATETIME_TYPE,
    "int": SINGER_INT_TYPE,
    "decimal": SINGER_DECIMAL_TYPE,
    "double": SINGER_FLOAT_TYPE,
    "float": SINGER_FLOAT_TYPE,
    "string": SINGER_STRING_TYPE,
    "test": SINGER_STRING_TYPE,
    "char": SINGER_STRING_TYPE,
}


class DatabaseStreamBase(TapStreamBase, metaclass=abc.ABCMeta):
    """Abstract base class for database-type streams.

    This class currently supports databases with 3-part names only. For databases which
    use two-part names, further modification to certain methods may be necessary.
    """

    MAX_CONNECT_RETRIES = 5
    THREE_PART_NAMES = True  # For backwards compatibility reasons

    @classproperty
    def table_scan_query(cls) -> str:
        """Return a SQL statement that provides the column."""
        return """
            SELECT catalog, schema_name, table_name
            from information_schema.tables
            WHERE UPPER(table_type) not like '%VIEW%'
            """

    @classproperty
    def view_scan_query(cls) -> str:
        """Return a SQL statement that provides the column."""
        return """
            SELECT catalog, schema_name, table_name
            FROM information_schema.views
            """

    @classproperty
    def column_scan_query(cls) -> str:
        """Return a SQL statement that provides the column."""
        return """
            SELECT
                  catalog, schema_name, table_name, column_name, data_type
            FROM information_schema.columns
            ORDER BY catalog, schema_name, table_name, ordinal
            """

    @staticmethod
    def create_singer_schema(columns: Dict[str, str],) -> singer.Schema:
        props: Dict[str, singer.Schema] = []
        for column, sql_type in columns.items():
            props[column] = DatabaseStreamBase.get_singer_type(sql_type=sql_type)
        return props

    @staticmethod
    def get_singer_type(sql_type: str) -> singer.Schema:
        for matchable, singer_type in SINGER_TYPE_LOOKUP:
            if matchable in sql_type:
                return singer_type
        raise RuntimeError("Could not infer a Singer data type from type '{sql_type}'.")

    @classmethod
    def scan_and_collate_columns(
        cls, config
    ) -> Dict[Tuple[str, str, str], Dict[str, str]]:
        columns_scan_result = cls.query(config=config, sql=cls.columns_scan_query)
        result: Dict[Tuple[str, str, str], Dict[str, str]] = {}
        for catalog, schema_name, table, column, data_type in columns_scan_result:
            result[(catalog, schema_name, table)][column] = data_type
        return result

    @classmethod
    def scan_primary_keys(cls, config) -> Dict[Tuple[str, str, str], List[str]]:
        columns_scan_result = cls.query(config=config, sql=cls.columns_scan_query)
        result: Dict[Tuple[str, str, str], Dict[str, str]] = {}
        for catalog, schema_name, table, column, data_type in columns_scan_result:
            result[(catalog, schema_name, table)][column] = data_type
        return result

    @classmethod
    def from_discovery(cls, config: dict) -> List[FactoryType]:
        """Return a list of all streams (tables)."""
        result: List[DatabaseStreamBase] = []
        table_scan_result = cls.query(config=config, sql=cls.table_scan_query)
        view_scan_result = cls.query(config=config, sql=cls.view_scan_query)
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
            name_tuple = database, schema_name, table
            full_name = ".".join(name_tuple)
            columns = collated_columns.get(name_tuple, None)
            singer_schema = cls.create_singer_schema(columns)
            if not columns:
                raise RuntimeError(f"Did not find any columns for table '{full_name}'")
            primary_keys = primary_keys_lookup.get(name_tuple, None)
            new_stream = DatabaseStreamBase(
                tap_stream_id="-".join(name_tuple),
                config=config,
                database=database,
                schema=singer_schema,
            )
            new_stream.primary_keys = primary_keys
            # TODO: Expanded metadata support for setting `row_count` and `is_view`.
            # new_stream.is_view = is_view
            # new_stream.row_count = row_count
            result.append(new_stream)

    @abc.abstractclassmethod
    def query(cls, config, sql: str) -> Iterable[dict]:
        """Run a SQL query and generate a dict for each returned row."""
        pass

    def enquote(self, identifier: str):
        """Escape identifier to be SQL safe."""
        for quotechar in [self.DEFAULT_QUOTE_CHAR] + self.OTHER_QUOTE_CHARS:
            if quotechar in identifier:
                raise Exception(
                    f"Can't escape identifier `{identifier}` because it contains a "
                    f"quote character ({quotechar})."
                )
        return f"{self.DEFAULT_QUOTE_CHAR}{identifier.upper()}{self.DEFAULT_QUOTE_CHAR}"

    def dequote(self, identifier: str):
        """Dequote identifier from quoted version."""
        for quotechar in [self.DEFAULT_QUOTE_CHAR] + self.OTHER_QUOTE_CHARS:
            if identifier.startswith(quotechar):
                return identifier.lstrip(quotechar).rstrip(quotechar)
