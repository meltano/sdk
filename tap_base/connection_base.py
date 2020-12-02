"""Shared parent class for TapBase, TargetBase, and TransformBase."""

import abc
from typing import Any, List, Optional

from singer import Catalog, CatalogEntry


class GenericConnectionBase(metaclass=abc.ABCMeta):
    """Abstract base class for generic tap connections."""

    _config: dict
    _conn: Any
    _is_discoverable: bool = False

    def __init__(self, config: dict):
        """Initialize connection."""
        self._config = config

    def get_config(self, config_key: str, default: Any = None) -> Any:
        """Return config value or a default value."""
        return self._config.get(config_key, default)

    @abc.abstractmethod
    def open_connection(self) -> Any:
        """Initialize the tap connection."""
        pass

    def is_connected(self) -> bool:
        """Return True if connected."""
        return self._conn is not None

    def ensure_connected(self):
        """Connect if not yet connected."""
        if not self.is_connected():
            self.open_connection()

    @abc.abstractmethod
    def get_available_stream_ids(self) -> List[str]:
        """Return a list of all streams (tables)."""
        pass

    def discover_catalog(self) -> Catalog:
        """Return a list of all streams (tables)."""
        raise NotImplementedError(
            "This connection type does not yet support automatic discovery."
        )


class DiscoverableConnectionBase(GenericConnectionBase, metaclass=abc.ABCMeta):
    """Abstract base class for (generic) connections that support discovery."""

    _is_discoverable = True
    _catalog: Catalog = None

    def get_catalog_entry(self, tap_stream_id: str) -> CatalogEntry:
        self._catalog.get_stream(tap_stream_id)

    def get_selected_catalog_entries(self, tap_stream_id: str) -> CatalogEntry:
        self._catalog.get_selected_streams()

    def discover_catalog(self) -> Catalog:
        """Return a list of all streams (tables)."""
        streams: List[CatalogEntry] = []
        for tap_stream_id in self.get_available_stream_ids():
            streams.append(self.discover_stream(tap_stream_id))
        self._catalog = Catalog(streams)
        return self._catalog

    @abc.abstractmethod
    def discover_stream(self, tap_stream_id) -> CatalogEntry:
        """Scan a specific stream and return its discovered CatalogEntry object."""
        pass


class DatabaseConnectionBase(DiscoverableConnectionBase, metaclass=abc.ABCMeta):
    """Abstract base class for database-type connections."""

    THREE_PART_NAMES: bool = True  # Uses db.schema.table syntax (versus 2-part: db.table)
    DEFAULT_QUOTE_CHAR = '"'
    OTHER_QUOTE_CHARS = ['"', "[", "]", "`"]

    def __init__(self, config: dict):
        """Initialize connection."""
        super().__init__(config=config)

    def get_config(self, config_key: str, default: Any = None) -> Any:
        """Return config value or a default value."""
        return self._config.get(config_key, default)

    def get_available_stream_ids(self) -> List[str]:
        """Return a list of all streams (tables)."""
        self.ensure_connected()
        if self.THREE_PART_NAMES:
            results = self.query(
                """SELECT catalog, schema_name, table_name from information_schema.tables"""
            )
            return [
                self.concatenate_tap_stream_id(
                    table_name=table, catalog_name=catalog, schema_name=schema
                )
                for catalog, schema, table in results
            ]
        else:
            results = self.query(
                """SELECT catalog, schema_name, table_name from information_schema.tables"""
            )
            return [
                self.concatenate_tap_stream_id(
                    table_name=table, catalog_name=catalog, schema_name=schema
                )
                for catalog, table, schema in results
            ]

    def discover_stream(self, tap_stream_id) -> CatalogEntry:
        """Scan a specific stream and return its discovered CatalogEntry object.

        Note: the default implementation assumes 3-part table naming and access to
        'information_schema' tables. For platforms which do not support
        'information_schema', this method will need to be overridden.
        """
        db, schema, table = tap_stream_id.split("-")
        _table_type = self.query(
            f"""
            SELECT table_type from information_schema.tables
            WHERE catalog = '{db}'
              AND schema  = '{schema}'
              AND table   = '{table}'
            """
        )
        est_rowcount: Optional[int] = None
        primary_key_cols: Optional[List[str]] = None
        is_view = _table_type not in ["TABLE"]
        return CatalogEntry(
            tap_stream_id=tap_stream_id,
            stream=tap_stream_id,
            key_properties=primary_key_cols,
            schema=None,
            replication_key=None,
            is_view=is_view,
            database=db,
            table=table,
            row_count=est_rowcount,
            stream_alias=tap_stream_id,
            metadata=None,
            replication_method=None,
        )

    def query(self, query: str):
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

    def concatenate_tap_stream_id(
        self, table_name: str, catalog_name: str, schema_name: str = None,
    ):
        """Generate tap stream id as appears in properties.json."""
        if schema_name:
            return catalog_name + "-" + schema_name + "-" + table_name
        else:
            return catalog_name + "-" + table_name
