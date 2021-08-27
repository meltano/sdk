# """Base class for database-type streams."""

import abc
import backoff

import singer
from typing import (
    Any,
    Dict,
    Iterable,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
)

from singer_sdk.exceptions import TapStreamConnectionFailure
from singer_sdk.plugin_base import PluginBase as TapBaseClass
from singer_sdk.streams.core import Stream
from singer_sdk import typing as th

CatalogFactoryType = TypeVar("CatalogFactoryType", bound="DatabaseCatalogFactory")
StreamFactoryType = TypeVar("StreamFactoryType", bound="DatabaseStream")


SQL_TYPE_LOOKUP: Dict[str, dict] = {
    # NOTE: This is an ordered mapping, with earlier mappings taking precedence.
    #       If the SQL-provided type contains the type name on the left, the mapping
    #       will return the respective singer type.
    "timestamp": th.DateTimeType.type_dict(),
    "datetime": th.DateTimeType.type_dict(),
    "date": th.DateTimeType.type_dict(),
    "int": th.IntegerType.type_dict(),
    "number": th.NumberType.type_dict(),
    "decimal": th.NumberType.type_dict(),
    "double": th.NumberType.type_dict(),
    "float": th.NumberType.type_dict(),
    "string": th.StringType.type_dict(),
    "text": th.StringType.type_dict(),
    "char": th.StringType.type_dict(),
    "bool": th.BooleanType.type_dict(),
    "variant": th.StringType.type_dict(),
}


def _get_catalog_entries(catalog_dict: dict) -> List[dict]:
    """Parse the catalog dict and return a list of catalog entries."""
    if "streams" not in catalog_dict:
        raise ValueError("Catalog does not contain expected 'streams' collection.")

    if not catalog_dict.get("streams"):
        raise ValueError("Catalog does not contain any streams.")

    return cast(List[dict], catalog_dict.get("streams"))


class DatabaseQueryService(metaclass=abc.ABCMeta):
    """Executes SQL queries and commands."""

    DEFAULT_QUOTE_CHAR = '"'
    OTHER_QUOTE_CHARS = ['"', "[", "]", "`"]
    MAX_CONNECT_RETRIES = 5

    def __init__(self, tap_config: dict) -> None:
        """Initialize the query service using the provided tap config."""
        pass

    @abc.abstractmethod
    def open_connection(self, config) -> Any:
        """Connect to the database source."""
        pass

    @abc.abstractmethod
    def execute_query(self, sql: Union[str, List[str]], config) -> Iterable[dict]:
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

    def connect_with_retries(self) -> Any:
        """Run open_stream_connection(), retry automatically a few times if failed."""
        return backoff.on_exception(
            backoff.expo,
            exception=TapStreamConnectionFailure,
            max_tries=self.MAX_CONNECT_RETRIES,
            on_backoff=self.log_backoff_attempt,
            factor=2,
        )(self.open_connection)()

    def log_backoff_attempt(cls, details):
        """Log backoff attempts used by stream retry_pattern()."""
        cls.logger.info(
            "Error communicating with source, "
            f"triggering backoff: {details.get('tries')} try"
        )


class DatabaseCatalogFactory(metaclass=abc.ABCMeta):
    """Generates the database catalog."""

    query_service_class: Type[DatabaseQueryService]

    def __init__(self, tap: TapBaseClass) -> None:
        self._query_service: Optional[DatabaseQueryService] = self.query_service_class(
            dict(tap.config)
        )

    @property
    def query_service(self) -> DatabaseQueryService:
        if not self._query_service:
            raise ValueError("Query service is not initialized.")

        return self._query_service

    @classmethod
    def _get_jsonschema(cls, columns: Dict[str, str]) -> dict:
        """Return a singer 'Schema' object with the specified columns and data types."""
        props: List[th.Property] = []
        for column, sql_type in columns.items():
            props.append(
                th.Property(column, th.CustomType(cls.to_jsonschema_type(sql_type)))
            )
        return th.PropertiesList(*props).to_dict()

    @classmethod
    def to_jsonschema_type(cls, sql_type: str) -> dict:
        """Return a singer type class based on the provided sql-base data type."""
        for matchable in SQL_TYPE_LOOKUP.keys():
            if matchable.lower() in sql_type.lower():
                return SQL_TYPE_LOOKUP[matchable]

        raise RuntimeError(
            f"Could not infer a Singer data type from type '{sql_type}'."
        )

    @property
    def table_scan_sql(self) -> str:
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

    @property
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

    @property
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

    @property
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

    def scan_and_collate_columns(
        self, config
    ) -> Dict[Tuple[str, str, str], Dict[str, str]]:
        """Return a mapping of columns and datatypes for each table and view."""
        columns_scan_result = self.query_service.execute_query(
            config=config, sql=self.column_scan_sql
        )
        result: Dict[Tuple[str, str, str], Dict[str, str]] = {}
        for row_dict in columns_scan_result:
            row_dict = cast(dict, row_dict)
            catalog, schema_name, table, column, data_type = row_dict.values()
            if (catalog, schema_name, table) not in result:
                result[(catalog, schema_name, table)] = {}
            result[(catalog, schema_name, table)][column] = data_type
        return result

    def scan_primary_keys(self, config) -> Dict[Tuple[str, str, str], List[str]]:
        """Return a listing of primary keys for each table and view."""
        result: Dict[Tuple[str, str, str], List[str]] = {}
        if not self.primary_key_scan_sql:
            return result

        pk_scan_result = self.query_service.execute_query(
            config=config, sql=self.primary_key_scan_sql
        )
        for row_dict in pk_scan_result:
            row_dict = cast(dict, row_dict)
            catalog, schema_name, table, pk_column = row_dict.values()
            if (catalog, schema_name, table) not in result:
                result[(catalog, schema_name, table)] = []
            result[(catalog, schema_name, table)].append(pk_column)
        return result

    def run_discovery(self, tap: TapBaseClass) -> singer.Catalog:
        """Return a list of catalog entry objects."""
        result: List[dict] = []
        config = tap.config
        table_scan_results = self.query_service.execute_query(
            config=config, sql=self.table_scan_sql
        )
        view_scan_results = self.query_service.execute_query(
            config=config, sql=self.view_scan_sql
        )
        all_results = [
            (database, schema_name, table, False)
            for database, schema_name, table in table_scan_results
        ] + [
            (database, schema_name, table, True)
            for database, schema_name, table in view_scan_results
        ]
        collated_columns = self.scan_and_collate_columns(config=config)
        primary_keys_lookup = self.scan_primary_keys(config=config)
        for database, schema_name, table, is_view in all_results:
            name_tuple = (database, schema_name, table)
            full_name = ".".join(name_tuple)
            columns = collated_columns.get(name_tuple, None)
            if not columns:
                raise RuntimeError(f"Did not find any columns for table '{full_name}'")

            jsonschema: dict = self._get_jsonschema(columns)
            primary_keys = primary_keys_lookup.get(name_tuple, None)
            metadata = cast(
                List[dict],
                singer.metadata.get_standard_metadata(
                    schema=jsonschema,
                    key_properties=primary_keys,
                    schema_name=None,
                    # replication_method=self.forced_replication_method,
                    # valid_replication_keys=(
                    #     [self.replication_key] if self.replication_key else None
                    # ),
                ),
            )
            new_catalog_entry = singer.CatalogEntry(
                tap_stream_id=full_name,
                name=full_name,
                key_properties=primary_keys,
                is_view=is_view,
                metadata=metadata,
            )
            result.append(new_catalog_entry)
        return result


class DatabaseStream(Stream, metaclass=abc.ABCMeta):
    """Abstract base class for database-type streams.

    This class currently supports databases with 3-part names only. For databases which
    use two-part names, further modification to certain methods may be necessary.
    """

    THREE_PART_NAMES = True  # For backwards compatibility reasons

    query_service_class: Type[DatabaseQueryService]

    def __init__(
        self,
        tap: TapBaseClass,
        catalog_entry: Dict[str, Any],
    ):
        """Initialize the database stream.

        Parameters
        ----------
        tap : TapBaseClass
            reference to the parent tap
        catalog_entry : Dict[str, Any]
            A schema dict or the path to a valid schema file in json.
        """
        self.query_service = self.query_service_class(dict(tap.config))
        self.is_view: Optional[bool] = catalog_entry.get("is-view", False)
        self.row_count: Optional[int] = None
        name = catalog_entry.get("tap_stream_id", catalog_entry.get("stream"))
        super().__init__(
            tap=tap,
            schema=catalog_entry["schema"],
            name=name,
        )

    def get_records(self, partition: Optional[dict]) -> Iterable[Dict[str, Any]]:
        """Return a generator of row-type dictionary objects.

        Each row emitted should be a dictionary of property names to their values.
        """
        if partition:
            raise NotImplementedError(
                f"Stream '{self.name}' does not support partitioning."
            )
        for row in self.query_service.execute_query(
            sql=f"SELECT * FROM {self.fully_qualified_name}", config=self.config
        ):
            yield row

    @property
    def fully_qualified_name(self):
        """Return the fully qualified name of the table name."""
        return self.tap_stream_id

    @classmethod
    def from_catalog(cls, tap: TapBaseClass, catalog: dict) -> List[StreamFactoryType]:
        """Initialize streams from an existing catalog, returning a list of streams."""
        result: List[StreamFactoryType] = []
        if not catalog:
            raise ValueError(
                "Could not initialize stream from blank or missing catalog."
            )
        for catalog_entry in _get_catalog_entries(catalog):
            new_stream = cast(
                StreamFactoryType,
                cls(
                    tap=tap,
                    catalog_entry=catalog_entry,
                ),
            )
            result.append(new_stream)
        return result
