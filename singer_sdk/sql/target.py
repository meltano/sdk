"""SQL Target implementation."""

from __future__ import annotations

import typing as t

from singer_sdk.helpers.capabilities import (
    TARGET_HARD_DELETE_CONFIG,
    TARGET_SCHEMA_CONFIG,
    PluginCapabilities,
    TargetCapabilities,
)
from singer_sdk.target_base import Target

if t.TYPE_CHECKING:
    from singer_sdk.helpers.capabilities import CapabilitiesEnum
    from singer_sdk.sinks.core import Sink
    from singer_sdk.sql.connector import SQLConnector
    from singer_sdk.sql.sink import SQLSink

__all__ = ["SQLTarget"]


class SQLTarget(Target):
    """Target implementation for SQL destinations."""

    _target_connector: SQLConnector | None = None

    default_sink_class: type[SQLSink]

    #: A list of capabilities supported by this target.
    capabilities: t.ClassVar[list[CapabilitiesEnum]] = [
        *Target.capabilities,
        PluginCapabilities.ACTIVATE_VERSION,
        TargetCapabilities.TARGET_SCHEMA,
        TargetCapabilities.HARD_DELETE,
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

    @classmethod
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

        def _merge_missing(source_jsonschema: dict, target_jsonschema: dict) -> None:
            # Append any missing properties in the target with those from source.
            for k, v in source_jsonschema["properties"].items():
                if k not in target_jsonschema["properties"]:
                    target_jsonschema["properties"][k] = v

        capabilities = cls.capabilities

        if TargetCapabilities.TARGET_SCHEMA in capabilities:
            _merge_missing(TARGET_SCHEMA_CONFIG, config_jsonschema)

        if TargetCapabilities.HARD_DELETE in capabilities:
            _merge_missing(TARGET_HARD_DELETE_CONFIG, config_jsonschema)

        super().append_builtin_config(config_jsonschema)

    @t.final
    def add_sqlsink(
        self,
        stream_name: str,
        schema: dict,
        key_properties: t.Sequence[str] | None = None,
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
        sink_class = self.get_sink_class(stream_name=stream_name)
        sink = sink_class(
            target=self,
            stream_name=stream_name,
            schema=schema,
            key_properties=key_properties,
            connector=self.target_connector,
        )
        sink.setup()
        self._sinks_active[stream_name] = sink

        return sink

    def get_sink_class(self, stream_name: str) -> type[SQLSink]:
        """Get sink for a stream.

        Developers can override this method to return a custom Sink type depending
        on the value of `stream_name`. Optional when `default_sink_class` is set.

        Args:
            stream_name: Name of the stream.

        Raises:
            ValueError: If no :class:`singer_sdk.sinks.Sink` class is defined.

        Returns:
            The sink class to be used with the stream.
        """
        if self.default_sink_class:
            return self.default_sink_class

        msg = (
            f"No sink class defined for '{stream_name}' and no default sink class "
            "available."
        )
        raise ValueError(msg)

    def get_sink(
        self,
        stream_name: str,
        *,
        record: dict | None = None,
        schema: dict | None = None,
        key_properties: t.Sequence[str] | None = None,
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
