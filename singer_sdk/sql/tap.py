"""SQL Tap implementation."""

from __future__ import annotations

import typing as t

from singer_sdk.configuration._dict_config import merge_missing_config_jsonschema
from singer_sdk.helpers.capabilities import SQL_TAP_USE_SINGER_DECIMAL
from singer_sdk.tap_base import Tap

if t.TYPE_CHECKING:
    from singer_sdk.sql.connector import SQLConnector
    from singer_sdk.sql.stream import SQLStream
    from singer_sdk.streams.core import Stream

__all__ = ["SQLTap"]


class SQLTap(Tap):
    """A specialized Tap for extracting from SQL streams."""

    default_stream_class: type[SQLStream]
    """
    The default stream class used to initialize new SQL streams from their catalog
    entries.
    """

    dynamic_catalog: bool = True
    """
    Whether the tap's catalog is dynamic, enabling configuration validation in
    discovery mode. Set to True if the catalog is generated dynamically (e.g. by
    querying a database's system tables).
    """

    exclude_schemas: t.Sequence[str] = []
    """Hard-coded list of stream names to skip when discovering the catalog."""

    _tap_connector: SQLConnector | None = None

    def __init__(self, *args: t.Any, **kwargs: t.Any) -> None:
        """Initialize the SQL tap.

        The SQLTap initializer additionally creates a cache variable for _catalog_dict.

        Args:
            *args: Positional arguments for the Tap initializer.
            **kwargs: Keyword arguments for the Tap initializer.
        """
        self._catalog_dict: dict[str, list[dict]] | None = None
        super().__init__(*args, **kwargs)

    @classmethod
    def append_builtin_config(cls, config_jsonschema: dict) -> None:
        """Appends built-in config to `config_jsonschema` if not already set.

        Args:
            config_jsonschema: [description]
        """
        merge_missing_config_jsonschema(SQL_TAP_USE_SINGER_DECIMAL, config_jsonschema)
        super().append_builtin_config(config_jsonschema)

    @property
    def tap_connector(self) -> SQLConnector:
        """The connector object.

        Returns:
            The connector object.
        """
        if self._tap_connector is None:
            self._tap_connector = self.default_stream_class.connector_class(
                dict(self.config),
            )
        return self._tap_connector

    @property
    def catalog_dict(self) -> dict:
        """Get catalog dictionary.

        Returns:
            The tap's catalog as a dict
        """
        if self._catalog_dict is not None:
            return self._catalog_dict

        if self.input_catalog:
            return self.input_catalog.to_dict()

        connector = self.tap_connector

        self._catalog_dict = {
            "streams": connector.discover_catalog_entries(
                exclude_schemas=self.exclude_schemas
            )
        }
        return self._catalog_dict

    def discover_streams(self) -> t.Sequence[Stream]:
        """Initialize all available streams and return them as a sequence.

        Returns:
            A sequence of discovered Stream objects.
        """
        return [
            self.default_stream_class(
                tap=self,
                catalog_entry=catalog_entry,
                connector=self.tap_connector,
            )
            for catalog_entry in self.catalog_dict["streams"]
        ]
