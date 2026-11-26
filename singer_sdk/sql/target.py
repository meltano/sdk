"""SQL Target implementation."""

from __future__ import annotations

__lazy_modules__ = {
    "datetime",
    "singer_sdk.configuration._dict_config",
    "singer_sdk.singerlib.json",
    "singer_sdk.singerlib.types",
    "singer_sdk.sinks.core",
    "singer_sdk.sql.connector",
    "singer_sdk.sql.sink",
    "sqlalchemy",
}

import datetime
import functools
import sys
import typing as t

import sqlalchemy as sa
import sqlalchemy.exc

from singer_sdk.configuration._dict_config import merge_missing_config_jsonschema
from singer_sdk.helpers.capabilities import (
    PluginCapabilities,
    TargetCapabilities,
    _hard_delete_config,
    _merge_schemas,
    _target_schema_config,
    target_config_for_capabilities,
)
from singer_sdk.singerlib.json import serialize_json
from singer_sdk.target_base import Target

if sys.version_info >= (3, 12):
    from typing import override  # noqa: ICN003
else:
    from typing_extensions import override

if t.TYPE_CHECKING:
    from singer_sdk.helpers.capabilities import CapabilitiesEnum
    from singer_sdk.singerlib.types import KeyProperties
    from singer_sdk.sinks.core import Sink
    from singer_sdk.sql.connector import SQLConnector
    from singer_sdk.sql.sink import SQLSink

__all__ = ["SQLTarget"]


def sql_target_config_for_capabilities(
    capabilities: t.Iterable[CapabilitiesEnum],
) -> dict[str, t.Any]:
    """Generate the config JSON schema for SQL target capabilities.

    Args:
        capabilities: The capabilities supported by the SQL target.

    Returns:
        A JSON schema with the config properties for the capabilities.
    """
    capabilities = set(capabilities)
    schemas: list[dict[str, t.Any]] = []
    if TargetCapabilities.TARGET_SCHEMA in capabilities:
        schemas.append(_target_schema_config())
    if TargetCapabilities.HARD_DELETE in capabilities:
        schemas.append(_hard_delete_config())
    schemas.append(target_config_for_capabilities(capabilities))
    return _merge_schemas(*schemas)


class SQLTarget(Target):
    """Target implementation for SQL destinations."""

    _target_connector: SQLConnector | None = None

    default_sink_class: type[SQLSink]  # ty: ignore[incompatible-mutable-override]

    #: A list of capabilities supported by this target.
    capabilities: t.ClassVar[t.Sequence[CapabilitiesEnum]] = [
        *Target.capabilities,
        PluginCapabilities.ACTIVATE_VERSION,
        TargetCapabilities.TARGET_SCHEMA,
        TargetCapabilities.HARD_DELETE,
        PluginCapabilities.STATE,
    ]

    @property
    def target_connector(self) -> SQLConnector:
        """The connector object.

        Returns:
            The connector object.
        """
        if self._target_connector is None:
            self._target_connector = self.default_sink_class.connector_class(
                dict(self.config),
            )
        return self._target_connector

    @property
    def _state_namespace(self) -> str:
        namespace: str = self.config.get("state_namespace") or self.namespace
        return namespace

    @functools.cached_property
    def _state_table(self) -> sa.Table:
        # Create the schema and the table on the first state write only, so that
        # later drains in the same run skip the checks.
        table = sa.Table(
            "state",
            sa.MetaData(schema=self._state_namespace),
            sa.Column("state_id", sa.String(255), primary_key=True),
            sa.Column("state", sa.Text, nullable=False),
            sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        )
        connector = self.target_connector
        try:
            connector.prepare_schema(self._state_namespace)
        except sa.exc.DBAPIError:
            # A pipeline that runs in parallel can create the schema first.
            if not connector.schema_exists(self._state_namespace):
                raise
        with connector._connect() as conn, conn.begin():  # noqa: SLF001
            conn.execute(sa.schema.CreateTable(table, if_not_exists=True))
        return table

    @override
    def write_state(self, state_id: str, state: dict) -> None:
        table = self._state_table
        with self.target_connector._connect() as conn, conn.begin():  # noqa: SLF001
            conn.execute(table.delete().where(table.c.state_id == state_id))
            conn.execute(
                table.insert().values(
                    state_id=state_id,
                    state=serialize_json(state),
                    updated_at=datetime.datetime.now(datetime.timezone.utc),
                ),
            )
        self.logger.info("Wrote state '%s' to %s", state_id, table.fullname)

    @classmethod
    @override
    def append_builtin_config(cls, config_jsonschema: dict) -> None:
        """Appends built-in config to `config_jsonschema` if not already set.

        To customize or disable this behavior, developers may either override this class
        method or override the `capabilities` property to disabled any unwanted
        built-in capabilities.

        For all except very advanced use cases, we recommend leaving these
        implementations "as-is", since this provides the most choice to users and is
        the most "future proof" in terms of taking advantage of built-in capabilities
        which may be added in the future.

        Args:
            config_jsonschema: [description]
        """
        merge_missing_config_jsonschema(
            sql_target_config_for_capabilities(cls.capabilities),
            config_jsonschema,
        )

    @override
    def create_sink(
        self,
        *,
        stream_name: str,
        schema: dict,
        key_properties: KeyProperties | None = None,
    ) -> Sink:
        """Instantiate a new SQL sink for the given stream.

        Injects ``self.target_connector`` so all sinks share a single connection.
        Override to pass additional constructor arguments.

        Args:
            stream_name: Name of the stream.
            schema: Schema of the stream.
            key_properties: Primary key of the stream.

        Returns:
            A new sink instance for the stream.
        """
        sink_class = self.get_sink_class(stream_name=stream_name)
        return sink_class(
            target=self,
            stream_name=stream_name,
            schema=schema,
            key_properties=key_properties,
            connector=self.target_connector,
        )

    @t.final
    def add_sqlsink(
        self,
        stream_name: str,
        schema: dict,
        key_properties: KeyProperties | None = None,
    ) -> Sink:
        """Create a sink and register it.

        This method is internal to the SDK and should not need to be overridden.

        Args:
            stream_name: Name of the stream.
            schema: Schema of the stream.
            key_properties: Primary key of the stream.

        Returns:
            A new sink for the stream.
        """
        self.logger.debug("Initializing target sink '%s'...", self.name)
        sink = self.create_sink(
            stream_name=stream_name,
            schema=schema,
            key_properties=key_properties,
        )
        sink.setup()
        self._sinks_active[stream_name] = sink

        return sink

    @override
    def get_sink_class(self, stream_name: str) -> type[SQLSink]:
        """Get sink for a stream.

        Developers can override this method to return a custom Sink type depending
        on the value of `stream_name`. Optional when `default_sink_class` is set.

        Args:
            stream_name: Name of the stream.

        Returns:
            The sink class to be used with the stream.

        Raises:
            ValueError: If no :class:`singer_sdk.sinks.Sink` class is defined.
        """
        if self.default_sink_class:
            return self.default_sink_class

        msg = (
            f"No sink class defined for '{stream_name}' and no default sink class "
            "available."
        )
        raise ValueError(msg)

    @override
    def get_sink(
        self,
        stream_name: str,
        *,
        record: dict | None = None,
        schema: dict | None = None,
        key_properties: KeyProperties | None = None,
    ) -> Sink:
        """Return a sink for the given stream name.

        A new sink will be created if `schema` is provided and if either `schema` or
        `key_properties` has changed. If so, the old sink becomes archived and held
        until the next drain_all() operation.

        Developers only need to override this method if they want to provide a different
        sink depending on the values within the `record` object. Otherwise, please see
        `default_sink_class` property and/or the `get_sink_class()` method.

        Raises :class:`singer_sdk.exceptions.RecordsWithoutSchemaException` if sink does
        not exist and schema is not sent.

        Args:
            stream_name: Name of the stream.
            record: Record being processed.
            schema: Stream schema.
            key_properties: Primary key of the stream.

        Returns:
            The sink used for this target.
        """
        _ = record  # Custom implementations may use record in sink selection.
        if schema is None:
            self._assert_sink_exists(stream_name)
            return self._sinks_active[stream_name]

        existing_sink = self._sinks_active.get(stream_name, None)
        if not existing_sink:
            return self.add_sqlsink(stream_name, schema, key_properties)

        if (
            existing_sink.schema != schema
            or existing_sink.key_properties != key_properties
        ):
            self.logger.info(
                "Schema or key properties for '%s' stream have changed. "
                "Initializing a new '%s' sink...",
                stream_name,
                stream_name,
            )
            self._sinks_to_clear.append(self._sinks_active.pop(stream_name))
            return self.add_sqlsink(stream_name, schema, key_properties)

        return existing_sink
