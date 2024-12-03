from __future__ import annotations

import enum
import typing as t
import warnings

_EnumMemberT = t.TypeVar("_EnumMemberT")


class TargetLoadMethods(str, enum.Enum):
    """Target-specific capabilities."""

    # always write all input records whether that records already exists or not
    APPEND_ONLY = "append-only"

    # update existing records and insert new records
    UPSERT = "upsert"

    # delete all existing records and insert all input records
    OVERWRITE = "overwrite"


class DeprecatedEnum(enum.Enum):
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
        member.deprecation = deprecation
        return member

    @property
    def deprecation_message(self) -> str | None:
        """Get deprecation message.

        Returns:
            Deprecation message.
        """
        self.deprecation: str | None
        return self.deprecation

    def emit_warning(self) -> None:
        """Emit deprecation warning."""
        warnings.warn(
            f"{self.name} is deprecated. {self.deprecation_message}",
            DeprecationWarning,
            stacklevel=3,
        )


class DeprecatedEnumMeta(enum.EnumMeta):
    """Metaclass for enumeration with deprecation support."""

    def __getitem__(cls, name: str) -> t.Any:  # noqa: ANN401
        """Retrieve mapping item.

        Args:
            name: Item name.

        Returns:
            Enum member.
        """
        obj: enum.Enum = super().__getitem__(name)
        if isinstance(obj, DeprecatedEnum) and obj.deprecation_message:
            obj.emit_warning()
        return obj

    def __getattribute__(cls, name: str) -> t.Any:  # noqa: ANN401
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

    def __call__(cls, *args: t.Any, **kwargs: t.Any) -> t.Any:  # noqa: ANN401
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

    #: Support schema flattening, aka de-nesting of complex properties.
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

    #: Allow setting the target schema.
    TARGET_SCHEMA = "target-schema"

    #: Validate the schema of the incoming records.
    VALIDATE_RECORDS = "validate-records"
