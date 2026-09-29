"""Module with helpers to declare capabilities and plugin behavior."""

from __future__ import annotations

import sys
import typing as t
from enum import Enum, EnumMeta
from warnings import warn

from singer_sdk.typing import (
    AnyOf,
    ArrayType,
    BooleanType,
    Constant,
    DecimalType,
    IntegerType,
    NullType,
    ObjectType,
    OneOf,
    PropertiesList,
    Property,
    StringType,
)

_EnumMemberT = t.TypeVar("_EnumMemberT")

if sys.version_info >= (3, 12):
    from typing import override  # noqa: ICN003
else:
    from typing_extensions import override


# Builders for the JSON Schema of the config supporting built-in capabilities:


def _stream_maps_config() -> dict[str, t.Any]:
    """Build the JSON schema for `STREAM_MAPS_CONFIG`.

    Returns:
        The JSON schema.
    """
    return PropertiesList(
        Property(
            "stream_maps",
            ObjectType(
                Property(
                    "__else__",
                    AnyOf(Constant("__NULL__"), NullType()),
                    nullable=True,
                    required=False,
                    title="Other streams",
                    description=(
                        "Currently, only setting this to `__NULL__` is supported. "
                        "This will remove all other streams."
                    ),
                ),
                # Stream names → Stream map config
                additional_properties=AnyOf(
                    Constant("__NULL__"),  # Remove the stream.
                    NullType(),  # Remove the stream.
                    ObjectType(  # Map the stream using this configuration.
                        Property(
                            "__alias__",
                            StringType,
                            title="Stream Alias",
                            description="Alias to use for the stream.",
                            nullable=False,
                            required=False,
                        ),
                        Property(
                            "__else__",
                            AnyOf(Constant("__NULL__"), NullType()),
                            title="Other properties",
                            description=(
                                "Currently, only setting this to `__NULL__` is "
                                "supported. "
                                "This will remove all other properties from the stream."
                            ),
                            required=False,
                            nullable=False,
                        ),
                        Property(
                            "__filter__",
                            StringType(),
                            title="Filter",
                            description=(
                                "Filter out records from a stream. A string expression "
                                "which must evaluate to `true` to include the record, "
                                "or "
                                "`false` to exclude it. Filter expressions should be "
                                "wrapped in `bool()` to ensure proper type conversion."
                            ),
                            nullable=False,
                            required=False,
                        ),
                        Property(
                            "__key_properties__",
                            ArrayType(StringType()),
                            title="Key Properties",
                            description="Primary key properties for the stream.",
                            nullable=False,
                            required=False,
                        ),
                        Property(
                            "__source__",
                            StringType,
                            description="Create a new stream from this source stream.",
                            nullable=False,
                            required=False,
                        ),
                        # Property names → Property map config
                        additional_properties=AnyOf(
                            Constant(
                                "__NULL__"
                            ),  # Remove the property from the stream.
                            NullType(),  # Remove the property from the stream.
                            StringType(),  # Compute the property using this expression.
                        ),
                    ),
                ),
            ),
            title="Stream Maps",
            description=(
                "Config object for stream maps capability. "
                "For more information check out "
                "[Stream Maps](https://sdk.meltano.com/en/latest/stream_maps.html)."
            ),
        ),
        Property(
            "stream_map_config",
            ObjectType(),
            title="User Stream Map Configuration",
            description="User-defined config values to be used within map expressions.",
        ),
        Property(
            "faker_config",
            ObjectType(
                Property(
                    "seed",
                    OneOf(DecimalType, StringType, BooleanType),
                    title="Faker Seed",
                    description=(
                        "Value to seed the Faker generator for deterministic output: "
                        "https://faker.readthedocs.io/en/master/#seeding-the-generator"
                    ),
                ),
                Property(
                    "locale",
                    OneOf(StringType, ArrayType(StringType())),
                    title="Faker Locale",
                    description=(
                        "One or more LCID locale strings to produce localized output "
                        "for: "
                        "https://faker.readthedocs.io/en/master/#localization"
                    ),
                ),
            ),
            title="Faker Configuration",
            description=(
                "Config for the [`Faker`](https://faker.readthedocs.io/en/master/) "
                "instance variable `fake` used within map expressions. Only applicable "
                "if "
                "the plugin specifies `faker` as an additional dependency (through the "
                "`singer-sdk` `faker` extra or directly)."
            ),
        ),
    ).to_dict()


def _flattening_config() -> dict[str, t.Any]:
    """Build the JSON schema for `FLATTENING_CONFIG`.

    Returns:
        The JSON schema.
    """
    return PropertiesList(
        Property(
            "flattening_enabled",
            BooleanType(),
            title="Enable Schema Flattening",
            description=(
                "'True' to enable schema flattening and automatically expand nested "
                "properties."
            ),
        ),
        Property(
            "flattening_max_depth",
            IntegerType(),
            title="Max Flattening Depth",
            description="The max depth to flatten schemas.",
        ),
        Property(
            "flattening_max_key_length",
            IntegerType(),
            title="Max Key Length",
            description="The maximum length of a flattened key.",
        ),
        Property(
            "flattening_separator",
            StringType(),
            title="Flattening Separator",
            description="The separator to use when flattening keys.",
        ),
    ).to_dict()


def _tap_batch_config() -> dict[str, t.Any]:
    """Build the `batch_config` schema for taps.

    Returns:
        The JSON schema with the `batch_config` property.
    """
    return PropertiesList(
        Property(
            "batch_config",
            title="Tap BATCH Configuration",
            description="Configuration for emitting BATCH messages.",
            wrapped=ObjectType(
                Property(
                    "encoding",
                    title="Batch Encoding Configuration",
                    description=(
                        "Specifies the format and compression of the batch files."
                    ),
                    wrapped=ObjectType(
                        Property(
                            "format",
                            StringType,
                            title="Batch Encoding Format",
                            description="Format to use for batch files.",
                            required=True,
                        ),
                        Property(
                            "compression",
                            StringType,
                            allowed_values=["gzip", "none"],
                            title="Batch Compression Format",
                            description="Compression format to use for batch files.",
                        ),
                    ),
                    required=True,
                ),
                Property(
                    "storage",
                    title="Batch Storage Configuration",
                    description=(
                        "Defines the storage layer to use when writing batch files"
                    ),
                    wrapped=ObjectType(
                        Property(
                            "root",
                            StringType,
                            nullable=False,
                            title="Batch Storage Root",
                            description="Root path to use when writing batch files.",
                        ),
                        Property(
                            "prefix",
                            StringType,
                            title="Batch Storage Prefix",
                            description="Prefix to use when writing batch files.",
                        ),
                    ),
                ),
            ),
        ),
    ).to_dict()


def _target_batch_config() -> dict[str, t.Any]:
    """Build the `batch_config` schema for targets.

    Returns:
        The JSON schema with the `batch_config` property.
    """
    return PropertiesList(
        Property(
            "batch_config",
            title="Target BATCH Configuration",
            description="Configuration for consuming BATCH messages.",
            wrapped=ObjectType(additional_properties=False),
        ),
    ).to_dict()


def _sql_tap_config() -> dict[str, t.Any]:
    """Build the JSON schema for `SQL_TAP_USE_SINGER_DECIMAL`.

    Returns:
        The JSON schema.
    """
    return PropertiesList(
        Property(
            "use_singer_decimal",
            BooleanType(),
            title="Use Singer Decimal",
            description=(
                "Whether to use use strings with `x-singer.decimal` format for "
                "decimals in the discovered schema. "
                "This is useful to avoid precision loss when working with large "
                "numbers."
            ),
        ),
    ).to_dict()


def _target_schema_config() -> dict[str, t.Any]:
    """Build the JSON schema for `TARGET_SCHEMA_CONFIG`.

    Returns:
        The JSON schema.
    """
    return PropertiesList(
        Property(
            "default_target_schema",
            StringType(),
            title="Default Target Schema",
            description=(
                "The default target database schema name to use for all streams."
            ),
        ),
    ).to_dict()


def _emit_activate_version_config() -> dict[str, t.Any]:
    """Build the JSON schema for `EMIT_ACTIVATE_VERSION_CONFIG`.

    Returns:
        The JSON schema.
    """
    return PropertiesList(
        Property(
            "emit_activate_version_messages",
            BooleanType,
            default=False,
            title="Emit `ACTIVATE_VERSION` messages",
            description=(
                "Whether to emit `ACTIVATE_VERSION` messages. If set to `True`, "
                "the tap will emit `ACTIVATE_VERSION` messages for each stream."
            ),
        ),
    ).to_dict()


def _activate_version_config() -> dict[str, t.Any]:
    """Build the JSON schema for `ACTIVATE_VERSION_CONFIG`.

    Returns:
        The JSON schema.
    """
    return PropertiesList(
        Property(
            "process_activate_version_messages",
            BooleanType,
            default=True,
            title="Process `ACTIVATE_VERSION` messages",
            description="Whether to process `ACTIVATE_VERSION` messages.",
        ),
    ).to_dict()


def _add_record_metadata_config() -> dict[str, t.Any]:
    """Build the JSON schema for `ADD_RECORD_METADATA_CONFIG`.

    Returns:
        The JSON schema.
    """
    return PropertiesList(
        Property(
            "add_record_metadata",
            BooleanType(),
            title="Add Record Metadata",
            description="Whether to add metadata fields to records.",
        ),
    ).to_dict()


def _hard_delete_config() -> dict[str, t.Any]:
    """Build the JSON schema for `TARGET_HARD_DELETE_CONFIG`.

    Returns:
        The JSON schema.
    """
    return PropertiesList(
        Property(
            "hard_delete",
            BooleanType(),
            title="Hard Delete",
            description="Hard delete records.",
            default=False,
        ),
    ).to_dict()


def _validate_records_config() -> dict[str, t.Any]:
    """Build the JSON schema for `TARGET_VALIDATE_RECORDS_CONFIG`.

    Returns:
        The JSON schema.
    """
    return PropertiesList(
        Property(
            "validate_records",
            BooleanType(),
            title="Validate Records",
            description="Whether to validate the schema of the incoming streams.",
            default=True,
        ),
    ).to_dict()


def _batch_size_rows_config() -> dict[str, t.Any]:
    """Build the JSON schema for `TARGET_BATCH_SIZE_ROWS_CONFIG`.

    Returns:
        The JSON schema.
    """
    return PropertiesList(
        Property(
            "batch_size_rows",
            IntegerType,
            title="Batch Size Rows",
            description="Maximum number of rows in each batch.",
        ),
    ).to_dict()


class TargetLoadMethods(str, Enum):
    """Target-specific capabilities."""

    # always write all input records whether that records already exists or not
    APPEND_ONLY = "append-only"

    # update existing records and insert new records
    UPSERT = "upsert"

    # delete all existing records and insert all input records
    OVERWRITE = "overwrite"


def _load_method_config() -> dict[str, t.Any]:
    """Build the JSON schema for `TARGET_LOAD_METHOD_CONFIG`.

    Returns:
        The JSON schema.
    """
    return PropertiesList(
        Property(
            "load_method",
            StringType(),
            description=(
                "The method to use when loading data into the destination. "
                "`append-only` will always write all input records whether that "
                "records already exists or not. `upsert` will update existing "
                "records and insert new records. `overwrite` will delete all "
                "existing records and insert all input records."
            ),
            allowed_values=[
                TargetLoadMethods.APPEND_ONLY,
                TargetLoadMethods.UPSERT,
                TargetLoadMethods.OVERWRITE,
            ],
            default=TargetLoadMethods.APPEND_ONLY,
        ),
    ).to_dict()


class DeprecatedEnum(Enum):
    """Base class for capabilities enumeration."""

    deprecation: str | None

    def __new__(  # noqa: PYI034
        cls,
        value: _EnumMemberT,
        deprecation: str | None = None,
    ) -> DeprecatedEnum:
        """Create a new enum member.

        Args:
            value: Enum member value.
            deprecation: Deprecation message.

        Returns:
            An enum member value.
        """
        member: DeprecatedEnum = object.__new__(cls)
        member._value_ = value
        member.deprecation = deprecation
        return member

    @property
    def deprecation_message(self) -> str | None:
        """Deprecation message."""
        return self.deprecation

    def emit_warning(self) -> None:
        """Emit deprecation warning."""
        warn(
            f"{self.name} is deprecated. {self.deprecation_message}",
            DeprecationWarning,
            stacklevel=3,
        )


class DeprecatedEnumMeta(EnumMeta):
    """Metaclass for enumeration with deprecation support."""

    @override
    def __getitem__(cls, name: str) -> t.Any:
        """Retrieve mapping item.

        Args:
            name: Item name.

        Returns:
            Enum member.
        """
        obj: Enum = super().__getitem__(name)  # ty: ignore[invalid-assignment]
        if isinstance(obj, DeprecatedEnum) and obj.deprecation_message:
            obj.emit_warning()
        return obj

    @override
    def __getattribute__(cls, name: str) -> t.Any:
        """Retrieve enum attribute.

        Args:
            name: Attribute name.

        Returns:
            Attribute.
        """
        obj = super().__getattribute__(name)
        if isinstance(obj, DeprecatedEnum) and obj.deprecation_message:
            obj.emit_warning()
        return obj

    @override
    def __call__(cls, *args: t.Any, **kwargs: t.Any) -> t.Any:
        """Call enum member.

        Args:
            args: Positional arguments.
            kwargs: Keyword arguments.

        Returns:
            Enum member.
        """
        obj = super().__call__(*args, **kwargs)
        if isinstance(obj, DeprecatedEnum) and obj.deprecation_message:
            obj.emit_warning()
        return obj


class CapabilitiesEnum(DeprecatedEnum, metaclass=DeprecatedEnumMeta):
    """Base capabilities enumeration."""

    @override
    def __str__(self) -> str:
        """String representation.

        Returns:
            Stringified enum value.
        """
        return str(self.value)

    @override
    def __repr__(self) -> str:
        """String representation.

        Returns:
            Stringified enum value.
        """
        return str(self.value)


class PluginCapabilities(CapabilitiesEnum):
    """Core capabilities which can be supported by taps and targets."""

    #: Support plugin capability and setting discovery.
    ABOUT = "about"

    #: Support :doc:`inline stream map transforms</stream_maps>`.
    STREAM_MAPS = "stream-maps"

    #: Support schema flattening, aka unnesting of complex properties.
    FLATTENING = "schema-flattening"

    #: Support the
    #: `ACTIVATE_VERSION <https://hub.meltano.com/singer/docs#activate-version>`_
    #: extension.
    ACTIVATE_VERSION = "activate-version"

    #: Input and output from
    #: `batched files <https://hub.meltano.com/singer/docs#batch>`_.
    #: A.K.A ``FAST_SYNC``.
    BATCH = "batch"

    #: Support structured logging with contextual information.
    STRUCTURED_LOGGING = "structured-logging"


class TapCapabilities(CapabilitiesEnum):
    """Tap-specific capabilities."""

    #: Generate a catalog with `--discover`.
    DISCOVER = "discover"

    #: Accept input catalog, apply metadata and selection rules.
    CATALOG = "catalog"

    #: Incremental refresh by means of state tracking.
    STATE = "state"

    #: Automatic connectivity and stream init test via :ref:`--test<Test connectivity>`.
    TEST = "test"

    #: Support for ``replication_method: LOG_BASED``. You can read more about this
    #: feature in `MeltanoHub <https://hub.meltano.com/singer/docs#log-based>`_.
    LOG_BASED = "log-based"

    #: Deprecated. Please use :attr:`~TapCapabilities.CATALOG` instead.
    PROPERTIES = "properties", "Please use CATALOG instead."


class TargetCapabilities(CapabilitiesEnum):
    """Target-specific capabilities."""

    #: Allows a ``soft_delete=True`` config option.
    #: Requires a tap stream supporting :attr:`PluginCapabilities.ACTIVATE_VERSION`
    #: and/or :attr:`TapCapabilities.LOG_BASED`.
    SOFT_DELETE = "soft-delete"

    #: Allows a ``hard_delete=True`` config option.
    #: Requires a tap stream supporting :attr:`PluginCapabilities.ACTIVATE_VERSION`
    #: and/or :attr:`TapCapabilities.LOG_BASED`.
    HARD_DELETE = "hard-delete"

    #: Fail safe for unknown JSON Schema types.
    DATATYPE_FAILSAFE = "datatype-failsafe"

    #: Allow setting the target schema.
    TARGET_SCHEMA = "target-schema"

    #: Validate the schema of the incoming records.
    VALIDATE_RECORDS = "validate-records"


def _merge_schemas(*schemas: dict[str, t.Any]) -> dict[str, t.Any]:
    """Merge the properties of multiple JSON schemas into a new object schema.

    Args:
        schemas: Object JSON schemas to merge.

    Returns:
        A new object JSON schema.
    """
    properties: dict[str, t.Any] = {}
    for schema in schemas:
        properties.update(schema.get("properties", {}))
    return {"type": "object", "properties": properties}


def config_for_capabilities(
    capabilities: t.Iterable[CapabilitiesEnum],
) -> dict[str, t.Any]:
    """Generate the config JSON schema for plugin capabilities.

    Args:
        capabilities: The capabilities supported by the plugin.

    Returns:
        A JSON schema with the config properties for the capabilities.
    """
    capabilities = set(capabilities)
    schemas: list[dict[str, t.Any]] = []
    if PluginCapabilities.STREAM_MAPS in capabilities:
        schemas.append(_stream_maps_config())
    if PluginCapabilities.FLATTENING in capabilities:
        schemas.append(_flattening_config())
    return _merge_schemas(*schemas)


def tap_config_for_capabilities(
    capabilities: t.Iterable[CapabilitiesEnum],
) -> dict[str, t.Any]:
    """Generate the config JSON schema for tap capabilities.

    Args:
        capabilities: The capabilities supported by the tap.

    Returns:
        A JSON schema with the config properties for the capabilities.
    """
    capabilities = set(capabilities)
    schemas = [config_for_capabilities(capabilities)]
    if PluginCapabilities.ACTIVATE_VERSION in capabilities:
        schemas.append(_emit_activate_version_config())
    if PluginCapabilities.BATCH in capabilities:
        schemas.append(_tap_batch_config())
    return _merge_schemas(*schemas)


def sql_tap_config_for_capabilities(
    capabilities: t.Iterable[CapabilitiesEnum],
) -> dict[str, t.Any]:
    """Generate the config JSON schema for SQL tap capabilities.

    Args:
        capabilities: The capabilities supported by the SQL tap.

    Returns:
        A JSON schema with the config properties for the capabilities.
    """
    return _merge_schemas(
        _sql_tap_config(),
        tap_config_for_capabilities(capabilities),
    )


def target_config_for_capabilities(
    capabilities: t.Iterable[CapabilitiesEnum],
) -> dict[str, t.Any]:
    """Generate the config JSON schema for target capabilities.

    Args:
        capabilities: The capabilities supported by the target.

    Returns:
        A JSON schema with the config properties for the capabilities.
    """
    capabilities = set(capabilities)
    schemas = [
        _add_record_metadata_config(),
        _load_method_config(),
        _batch_size_rows_config(),
    ]
    if PluginCapabilities.ACTIVATE_VERSION in capabilities:
        schemas.append(_activate_version_config())
    if PluginCapabilities.BATCH in capabilities:
        schemas.append(_target_batch_config())
    if TargetCapabilities.VALIDATE_RECORDS in capabilities:
        schemas.append(_validate_records_config())
    schemas.append(config_for_capabilities(capabilities))
    return _merge_schemas(*schemas)


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


# Module-level schemas, kept for backwards compatibility. Prefer the
# `*_config_for_capabilities` functions.
STREAM_MAPS_CONFIG = _stream_maps_config()
FLATTENING_CONFIG = _flattening_config()
BATCH_CONFIG = TAP_BATCH_CONFIG = _tap_batch_config()
"""Batch config schema for taps, which must always specify an encoding format."""
TARGET_BATCH_CONFIG = _target_batch_config()
"""Batch config schema for targets, which read the encoding off each BATCH message."""
SQL_TAP_USE_SINGER_DECIMAL = _sql_tap_config()
TARGET_SCHEMA_CONFIG = _target_schema_config()
EMIT_ACTIVATE_VERSION_CONFIG = _emit_activate_version_config()
ACTIVATE_VERSION_CONFIG = _activate_version_config()
ADD_RECORD_METADATA_CONFIG = _add_record_metadata_config()
TARGET_HARD_DELETE_CONFIG = _hard_delete_config()
TARGET_VALIDATE_RECORDS_CONFIG = _validate_records_config()
TARGET_BATCH_SIZE_ROWS_CONFIG = _batch_size_rows_config()
TARGET_LOAD_METHOD_CONFIG = _load_method_config()
