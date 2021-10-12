"""Base class for database-type streams."""

import abc
import sqlalchemy

import singer
from typing import (
    Any,
    Dict,
    Iterable,
    List,
    Optional,
    cast,
    Union,
)

from singer_sdk.plugin_base import PluginBase as TapBaseClass
from singer_sdk.streams.core import Stream
from singer_sdk import typing as th


class SQLStream(Stream, metaclass=abc.ABCMeta):
    """Base class for SQLAlchemy-based streams."""

    def __init__(
        self,
        tap: TapBaseClass,
        catalog_entry: Dict[str, Any],
        connection: Optional[sqlalchemy.Connection],
    ):
        """Initialize the database stream.

        If `connection` is omitted or lost, a new connection will be made.

        Args:
            tap: The parent tap object.
            catalog_entry: Catalog entry dict.
            connection: Optional connection to reuse.
        """
        self._sqlalchemy_engine = self.get_sqlalchemy_engine(dict(tap.config))
        # self.is_view: Optional[bool] = catalog_entry.get("is-view", False)
        # self.row_count: Optional[int] = None
        self.catalog_entry = catalog_entry
        super().__init__(
            tap=tap,
            schema=self.schema,
            name=self.tap_stream_id,
        )
        self._sqlalchemy_connection = connection

    @property
    def metadata(self) -> List[dict]:
        """Return metadata object (dict) as specified in the Singer spec.

        Metadata from an input catalog will override standard metadata.
        """
        return cast(List[dict], self.catalog_entry["metadata"])

    @property
    def schema(self) -> dict:
        """Return metadata object (dict) as specified in the Singer spec.

        Metadata from an input catalog will override standard metadata.
        """
        return cast(dict, self.catalog_entry["schema"])

    @property
    def sqlalchemy_engine(self) -> sqlalchemy.engine.Engine:
        """Return or set the SQLAlchemy engine object.

        Developers may optionally override `sqlalchemy_engine` property
        for purposes of caching and/or reuse.
        """
        if not self._sqlalchemy_engine:
            raise ValueError("SQLAlchemy engine object does not exist.")

        return self._sqlalchemy_engine

    @property
    def sqlalchemy_connection(self) -> sqlalchemy.Connection:
        """Return or set the SQLAlchemy connection object."""
        if not self._sqlalchemy_connection:
            self._sqlalchemy_connection = self.sqlalchemy_engine.connect()

        return self._sqlalchemy_connection

    @property
    def tap_stream_id(self) -> str:
        """Return the unique ID used by the tap to identify this stream.

        Generally, this is the same value as in `Stream.name`.

        In rare cases, such as for database types with multi-part names,
        this may be slightly different from `Stream.name`.
        """
        return self.catalog_entry.get("tap_stream_id", self.catalog_entry.get("stream"))

    @property
    def fully_qualified_name(self):
        """Return the fully qualified name of the table name.

        TODO: Needs handling for dialect-specific quoting logic
        TODO: Consider rewriting to use SQLAlchemy
        """
        table_name = self.catalog_entry.get("table", self.catalog_entry.get("stream"))
        md_map = singer.metadata.to_map(self.catalog_entry.get("metadata"))
        schema_name = md_map[()]["schema-name"]
        db_name = self.catalog_entry.get("database")
        result = table_name

        if schema_name:
            result = f"{schema_name}.{result}"
        if db_name:
            result = f"{db_name}.{result}"

        return result

    # Get records from stream

    def get_records(self, partition: Optional[dict]) -> Iterable[Dict[str, Any]]:
        """Return a generator of row-type dictionary objects.

        Each row emitted should be a dictionary of property names to their values.
        """
        conn: sqlalchemy.Connection = self.sqlalchemy_connection
        if partition:
            raise NotImplementedError(
                f"Stream '{self.name}' does not support partitioning."
            )

        for row in conn.execute(
            sqlalchemy.text(f"SELECT * FROM {self.fully_qualified_name}")
        ):
            yield dict(row)

    # Class Methods

    @classmethod
    def get_sqlalchemy_url(cls, tap_config: dict) -> str:
        """Return the SQLAlchemy URL string.

        Developers can generally override just one of the following:
        `get_sqlalchemy_engine()`, `get_sqlalchemy_url()`.
        """
        return cast(str, tap_config["sqlalchemy_url"])

    @classmethod
    def get_sqlalchemy_engine(cls, tap_config: dict) -> sqlalchemy.engine.Engine:
        """Return a new SQLAlchemy engine using the provided config.

        Developers can generally override just one of the following:
        `get_sqlalchemy_engine()`, `get_sqlalchemy_url()`.
        """
        url = cls.get_sqlalchemy_url(tap_config)
        return sqlalchemy.create_engine(url)

    @classmethod
    def to_jsonschema_type(cls, from_type: Union[type, str, Any]) -> dict:
        """Return a JSON Schema representation of the provided type.

        By default will call `typing.to_jsonschema_type()` for strings and Python types.

        Developers may override this method to accept additional input argument types,
        to support non-standard types, or to provide custom typing logic.
        """
        if isinstance(from_type, (type, str)):
            return th.to_jsonschema_type(from_type)

        raise ValueError(f"Unexpected type received: '{type(from_type).__name__}'")

    @classmethod
    def run_discovery(cls, tap_config) -> Dict[str, List[dict]]:
        """Return a catalog dict from discovery."""
        engine = cls.get_sqlalchemy_engine(tap_config)
        result: dict = {"streams": []}
        inspected = sqlalchemy.inspect(engine)
        for schema_name in inspected.get_schema_names():
            table_names = inspected.get_table_names(schema=schema_name)
            try:
                view_names = inspected.get_view_names(schema=schema_name)
            except NotImplementedError:
                # TODO: Handle `get_view_names()`` not implemented
                # self.logger.warning(
                #     "Provider does not support get_view_names(). "
                #     "Streams list may be incomplete or `is_view` may be unpopulated."
                # )
                view_names = []
            object_names = [(t, False) for t in table_names] + [
                (v, True) for v in view_names
            ]
            for table_name, is_view in object_names:
                # table_obj: sqlalchemy.Table = sqlalchemy.Table(
                #     table_name, schema=schema_name
                # )
                # inspected.reflect_table(table=table_obj)
                possible_primary_keys: List[List[str]] = []

                pk_def = inspected.get_pk_constraint(table_name, schema=schema_name)
                if pk_def:
                    possible_primary_keys.append(pk_def)

                for index_def in inspected.get_indexes(table_name, schema=schema_name):
                    if index_def.get("unique", False):
                        possible_primary_keys.append(index_def["column_names"])

                table_schema = th.PropertiesList()
                for column_def in inspected.get_columns(table_name, schema=schema_name):
                    column_name = column_def["name"]
                    is_nullable = column_def.get("nullable", False)
                    jsonschema_type: dict = cls.to_jsonschema_type(
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
                addl_replication_methods: List[str] = []
                key_properties = next(iter(possible_primary_keys), None)
                replication_method = next(
                    reversed(["FULL_TABLE"] + addl_replication_methods)
                )
                catalog_entry = singer.CatalogEntry(
                    tap_stream_id=table_name,  # TODO: resolve dupes in multiple schemas
                    stream=table_name,
                    table=table_name,
                    key_properties=key_properties,
                    schema=singer.Schema.from_dict(schema),
                    is_view=is_view,
                    replication_method=replication_method,
                    metadata=singer.metadata.get_standard_metadata(
                        schema_name=schema_name,
                        schema=schema,
                        replication_method=replication_method,
                        key_properties=key_properties,
                        valid_replication_keys=None,  # Must be defined by user
                    ),
                    database=None,  # TODO: Detect database name
                    row_count=None,
                    stream_alias=None,
                    replication_key=None,  # Must be defined by user
                )
                result["streams"].append(catalog_entry.to_dict())

        return result
