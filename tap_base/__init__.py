"""tap-base library for building singer-compliant taps."""

from tap_base.plugin_base import PluginBase
from tap_base.tap_base import Tap
from tap_base import streams
from tap_base.streams import (
    Stream,
    DatabaseStream,
    RESTStream,
    GraphQLStream,
)

__all__ = [
    "PluginBase",
    "Tap",
    "streams",
    "Stream",
    "DatabaseStream",
    "RESTStream",
    "GraphQLStream",
]
