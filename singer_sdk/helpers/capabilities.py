"""Module with helpers to declare capabilities and plugin behavior."""

from __future__ import annotations

import typing as t
from enum import Enum, EnumMeta
from warnings import warn

from singer_sdk.typing import (
    BooleanType,
    IntegerType,
    ObjectType,
    PropertiesList,
    Property,
    StringType,
)

_EnumMemberT = t.TypeVar("_EnumMemberT")

# Default JSON Schema to support config for built-in capabilities:

STREAM_MAPS_CONFIG = PropertiesList(
    Property(
        "stream_maps",
        ObjectType(),
        description=(
            "Config object for stream maps capability. "
            "For more information check out "
            "[Stream Maps](https://sdk.meltano.com/en/latest/stream_maps.html)."
        ),
    ),
    Property(
        "stream_map_config",
        ObjectType(),
        description="User-defined config values to be used within map expressions.",
    ),
).to_dict()
FLATTENING_CONFIG = PropertiesList(
    Property(
        "flattening_enabled",
        BooleanType(),
        description=(
            "'True' to enable schema flattening and automatically expand nested "
            "properties."
        ),
    ),
    Property(
        "flattening_max_depth",
        IntegerType(),
        description="The max depth to flatten schemas.",
    ),
).to_dict()
BATCH_CONFIG = PropertiesList(
    Property(
        "batch_config",
        description="",
        wrapped=ObjectType(
            Property(
                "encoding",
                description="Specifies the format and compression of the batch files.",
                wrapped=ObjectType(
                    Property(
                        "format",
                        StringType,
                        allowed_values=["jsonl"],
                        description="Format to use for batch files.",
                    ),
                    Property(
                        "compression",
                        StringType,
                        allowed_values=["gzip", "none"],
                        description="Compression format to use for batch files.",
                    ),
                ),
            ),
            Property(
                "storage",
                description="Defines the storage layer to use when writing batch files",
                wrapped=ObjectType(
                    Property(
                        "root",
                        StringType,
                        description="Root path to use when writing batch files.",
                    ),
                    Property(
                        "prefix",
                        StringType,
                        description="Prefix to use when writing batch files.",
                    ),
                ),
            ),
        ),
    ),
).to_dict()
TARGET_SCHEMA_CONFIG = PropertiesList(
    Property(
        "default_target_schema",
        StringType(),
        description="The default target database schema name to use for all streams.",
    ),
).to_dict()
ADD_RECORD_METADATA_CONFIG = PropertiesList(
    Property(
        "add_record_metadata",
        BooleanType(),
        description="Add metadata to records.",
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


TARGET_LOAD_METHOD_CONFIG = PropertiesList(
    Property(
        "load_method",
        StringType(),
        description=(
            "The method to use when loading data into the destination. "
            "`append-only` will always write all input records whether that records "
            "already exists or not. `upsert` will update existing records and insert "
            "new records. `overwrite` will delete all existing records and insert all "
            "input records."
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

    def __new__(
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
        member._deprecation = deprecation
        return member

    @property
    def deprecation_message(self) -> str | None:
        """Get deprecation message.

        Returns:
            Deprecation message.
        """
        self._deprecation: str | None
        return self._deprecation

    def emit_warning(self) -> None:
        """Emit deprecation warning."""
        warn(
            f"{self.name} is deprecated. {self.deprecation_message}",
            DeprecationWarning,
            stacklevel=3,
        )


class DeprecatedEnumMeta(EnumMeta):
    """Metaclass for enumeration with deprecation support."""

    def __getitem__(self, name: str) -> t.Any:  # noqa: ANN401
        """Retrieve mapping item.

        Args:
            name: Item name.

        Returns:
            Enum member.
        """
        obj: Enum = super().__getitem__(name)
        if isinstance(obj, DeprecatedEnum) and obj.deprecation_message:
            obj.emit_warning()
        return obj

    def __getattribute__(cls, name: str) -> t.Any:  # noqa: ANN401, N805
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

    def __call__(self, *args: t.Any, **kwargs: t.Any) -> t.Any:  # noqa: ANN401
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

    def __str__(self) -> str:
        """String representation.

        Returns:
            Stringified enum value.
        """
        return str(self.value)

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

    #: Support schema flattening, aka denesting of complex properties.
    FLATTENING = "schema-flattening"

    #: Support the
    #: `ACTIVATE_VERSION <https://hub.meltano.com/singer/docs#activate-version>`_
    #: extension.
    ACTIVATE_VERSION = "activate-version"

    #: Input and output from
    #: `batched files <https://hub.meltano.com/singer/docs#batch>`_.
    #: A.K.A ``FAST_SYNC``.
    BATCH = "batch"


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

    #: Allow denesting complex properties.
    RECORD_FLATTENING = "record-flattening"

    #: Allow setting the target schema.
    TARGET_SCHEMA = "target-schema"
