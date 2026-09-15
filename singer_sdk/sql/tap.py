"""SQL Tap implementation."""

from __future__ import annotations

import inspect
import sys
import typing as t

from singer_sdk.configuration._dict_config import merge_missing_config_jsonschema
from singer_sdk.helpers.capabilities import (
    SQL_TAP_FILTER_SCHEMAS,
    SQL_TAP_USE_SINGER_DECIMAL,
)
from singer_sdk.tap_base import Tap

if sys.version_info >= (3, 12):
    from typing import override  # noqa: ICN003
else:
    from typing_extensions import override

if t.TYPE_CHECKING:
    from singer_sdk.sql.connector import SQLConnector
    from singer_sdk.sql.stream import SQLStream
    from singer_sdk.streams.core import Stream

__all__ = ["SQLTap"]

_DISCOVERY_KWARGS_CACHE: dict[type[SQLConnector], frozenset[str] | None] = {}


def _accepted_discovery_kwargs(
    connector_type: type[SQLConnector],
) -> frozenset[str] | None:
    """Return discovery kwargs accepted by the connector type.

    Returns:
        Accepted keyword names, or ``None`` if all kwargs are accepted.
    """
    if connector_type in _DISCOVERY_KWARGS_CACHE:
        return _DISCOVERY_KWARGS_CACHE[connector_type]

    signature = inspect.signature(connector_type.discover_catalog_entries)
    if any(
        param.kind is inspect.Parameter.VAR_KEYWORD
        for param in signature.parameters.values()
    ):
        _DISCOVERY_KWARGS_CACHE[connector_type] = None
        return _DISCOVERY_KWARGS_CACHE[connector_type]

    _DISCOVERY_KWARGS_CACHE[connector_type] = frozenset(signature.parameters)
    return _DISCOVERY_KWARGS_CACHE[connector_type]


def _filter_discovery_kwargs(
    connector: SQLConnector,
    discovery_kwargs: dict[str, t.Any],
) -> dict[str, t.Any]:
    """Filter discovery kwargs for custom connector method compatibility.

    Returns:
        Discovery kwargs accepted by the connector method signature.
    """
    accepted_kwargs = _accepted_discovery_kwargs(type(connector))
    if accepted_kwargs is None:
        return discovery_kwargs

    return {
        name: value
        for name, value in discovery_kwargs.items()
        if name in accepted_kwargs
    }


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
    @override
    def append_builtin_config(cls, config_jsonschema: dict) -> None:
        """Appends built-in config to `config_jsonschema` if not already set.

        Args:
            config_jsonschema: [description]
        """
        merge_missing_config_jsonschema(SQL_TAP_FILTER_SCHEMAS, config_jsonschema)
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
    @override
    def catalog_dict(self) -> dict:
        """Stream catalog dictionary."""
        if self._catalog_dict is not None:
            return self._catalog_dict

        if self.input_catalog:
            return self.input_catalog.to_dict()

        connector = self.tap_connector

        discovery_kwargs = {
            "exclude_schemas": self.exclude_schemas,
            "filter_schemas": self.config.get("filter_schemas") or [],
        }
        self._catalog_dict = {
            "streams": connector.discover_catalog_entries(
                **_filter_discovery_kwargs(connector, discovery_kwargs)
            )
        }
        return self._catalog_dict

    @override
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
