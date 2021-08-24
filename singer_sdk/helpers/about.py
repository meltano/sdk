"""Module with helpers to declare capabilities and plugin behavior."""

from enum import Enum


class PluginCapabilities(str, Enum):
    """Core capabilities which can be supported by taps and targets."""

    # Supported by default:
    ABOUT = "about"
    STREAM_MAPS = "stream-maps"

    # Not supported by default:
    ACTIVATE_VERSION = "activate-version"


class TapCapabilities(PluginCapabilities, Enum):
    """Tap-specific capabilities."""

    # Supported by default:
    CATALOG = "catalog"
    PROPERTIES = "properties"
    DISCOVER = "discover"
    STATE = "state"

    # Not supported by default:
    LOG_BASED = "log-based"


class TargetCapabilities(PluginCapabilities, Enum):
    """Target-specific capabilities."""

    # Not supported by default:
    SOFT_DELETE = "soft-delete"
    HARD_DELETE = "hard-delete"
