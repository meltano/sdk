"""Shared parent class for TapBase, TargetBase, and TransformBase."""

import abc
import logging
from typing import Any, List, Optional

from singer import Catalog, CatalogEntry
import backoff

from tap_base.exceptions import TapStreamConnectionFailure


class GenericStreamBase(metaclass=abc.ABCMeta):
    """Abstract base class for generic tap streams."""

    MAX_CONNECT_RETRIES = 0

    _config: dict
    _conn: Any
    _is_discoverable: bool = False

    logger: logging.Logger

    def __init__(self, config: dict, logger: logging.Logger):
        """Initialize stream."""
        self._config = config
        self.logger = logger

    def get_config(self, config_key: str, default: Any = None) -> Any:
        """Return config value or a default value."""
        return self._config.get(config_key, default)

    @abc.abstractmethod
    def open_stream_connection(self) -> Any:
        """Initialize the tap stream connection."""
        pass

    def log_backoff_attempt(self, details):
        """Log backoff attempts used by stream retry_pattern()."""
        self.logger.info(
            "Error communicating with source, "
            f"triggering backoff: {details.get('tries')} try"
        )

    def connect_with_retries(self) -> Any:
        """Run open_stream_connection() and retry automatically a few times if failed."""
        return backoff.on_exception(
            backoff.expo,
            exception=TapStreamConnectionFailure,
            max_tries=self.MAX_CONNECTION_RETRIES,
            on_backoff=self.log_backoff_attempt,
            factor=2,
        )(self.open_connection)()

    def is_connected(self) -> bool:
        """Return True if connected."""
        return self._conn is not None

    def ensure_connected(self):
        """Connect if not yet connected."""
        if not self.is_connected():
            self.connect_with_retries()

    @abc.abstractmethod
    def discover_available_stream_ids(self) -> List[str]:
        """Return a list of all streams (tables)."""
        pass

    def discover_catalog(self) -> Catalog:
        """Return a list of all streams (tables)."""
        raise NotImplementedError(
            "This connection type does not yet support automatic discovery."
        )


class DiscoverableStreamBase(GenericStreamBase, metaclass=abc.ABCMeta):
    """Abstract base class for (generic) streams that support discovery."""

    MAX_CONNECT_RETRIES = 0

    _is_discoverable = True

    def discover_catalog(self) -> Catalog:
        """Return a list of all streams (tables)."""
        streams: List[CatalogEntry] = []
        for tap_stream_id in self.discover_available_stream_ids():
            streams.append(self.discover_stream(tap_stream_id))
        return Catalog(streams)

    @abc.abstractmethod
    def discover_stream(self, tap_stream_id) -> CatalogEntry:
        """Scan a specific stream and return its discovered CatalogEntry object."""
        pass


class DatabaseStreamBase(DiscoverableStreamBase, metaclass=abc.ABCMeta):
    """Abstract base class for database-type streams."""

    MAX_CONNECT_RETRIES = 5

    THREE_PART_NAMES: bool = True  # Uses db.schema.table syntax (versus 2-part: db.table)
    DEFAULT_QUOTE_CHAR = '"'
    OTHER_QUOTE_CHARS = ['"', "[", "]", "`"]

    def get_config(self, config_key: str, default: Any = None) -> Any:
        """Return config value or a default value."""
        return self._config.get(config_key, default)

    def discover_available_stream_ids(self) -> List[str]:
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
