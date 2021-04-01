"""singer-sdk library for building singer-compliant taps."""

from singer_sdk.plugin_base import PluginBase
from singer_sdk.tap_base import Tap
from singer_sdk import streams
from singer_sdk.streams import (
    Stream,
    RESTStream,
    GraphQLStream,
)

__all__ = [
    "PluginBase",
    "Tap",
    "streams",
    "Stream",
    "RESTStream",
    "GraphQLStream",
]
