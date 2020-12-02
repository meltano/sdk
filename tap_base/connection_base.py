"""Shared parent class for TapBase, TargetBase, and TransformBase."""

import abc
from typing import Any, List


class GenericConnectionBase(metaclass=abc.ABCMeta):
    """Abstract base class for generic tap connections."""

    _config: dict
    _conn: Any

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


class DatabaseConnectionBase(GenericConnectionBase, metaclass=abc.ABCMeta):
    """Abstract base class for database-type connections."""

    THREE_PART_NAMES: bool = False  # Uses db.schema.table syntax (versus 2-part: db.table)
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
        if THREE_PART_NAMES:
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
                """SELECT catalog, table_name from information_schema.tables"""
            )
            return [
                self.concatenate_tap_stream_id(table_name=table, catalog_name=catalog)
                for catalog, table in results
            ]

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

