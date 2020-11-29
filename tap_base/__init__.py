"""tap-base library for building singer-compliant taps."""

from tap_base.plugin_base import PluginBase
from tap_base.tap_base import TapBase
from tap_base.tap_stream_base import TapStreamBase
from tap_base.connection_base import (
    GenericConnectionBase,
    DatabaseConnectionBase,
)

__all__ = [
    "PluginBase",
    "TapBase",
    "TapStreamBase",
    "GenericConnectionBase",
    "DatabaseConnectionBase",
]
