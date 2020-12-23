"""tap-base library for building singer-compliant taps."""

from tap_base.plugin_base import PluginBase
from tap_base.tap_base import TapBase
from tap_base import streams
from tap_base.streams import (
    TapStreamBase,
    DatabaseStreamBase,
    RESTStreamBase,
    GraphQLStreamBase,
)

__all__ = [
    "PluginBase",
    "TapBase",
    "streams",
    "TapStreamBase",
    "DatabaseStreamBase",
    "RESTStreamBase",
    "GraphQLStreamBase",
]
