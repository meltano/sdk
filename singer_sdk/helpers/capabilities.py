"""Module with helpers to declare capabilities and plugin behavior."""

from enum import Enum


class PluginCapabilities(str, Enum):
    """Core capabilities which can be supported by taps and targets."""

    # Supported by default:
    ABOUT = "about"  # Support capability and setting discovery
    STREAM_MAPS = "stream-maps"  # Support inline stream map transforms

    # Not supported by default:
    ACTIVATE_VERSION = "activate-version"  # Support the ACTIVATE_VERSION extension


class TapCapabilities(str, Enum):
    """Tap-specific capabilities."""

    # Supported by default:

    DISCOVER = "discover"  # Generate a catalog with `--discover`
    CATALOG = "catalog"  # Accept input catalog, apply metadata and selection rules
    STATE = "state"  # Incremental refresh by means of state tracking
    TEST = "test"  # Automatic connectivity and stream init test via `--test`

    # Not supported by default:
    LOG_BASED = "log-based"  # Support for `replication_method='LOG_BASED'`
    PROPERTIES = "properties"  # Deprecated. Please use 'catalog' instead


class TargetCapabilities(str, Enum):
    """Target-specific capabilities."""

    # Not supported by default:
    SOFT_DELETE = "soft-delete"  # Allows a `soft_delete=True` config option
    HARD_DELETE = "hard-delete"  # Allows a `hard_delete=True` config option
    DATATYPE_FAILSAFE = "datatype-failsafe"  # Fail safe for unknown JSON Schema types
    RECORD_FLATTENING = "record-flattening"  # Allow denesting complex properties
