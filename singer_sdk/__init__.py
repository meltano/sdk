"""SDK for building singer-compliant Singer taps."""

from singer_sdk.plugin_base import PluginBase
from singer_sdk.tap_base import Tap
from singer_sdk.target_base import Target
from singer_sdk import streams
from singer_sdk.streams import (
    Stream,
    RESTStream,
    GraphQLStream,
)
from singer_sdk.sinks import (
    Sink,
    RecordSink,
    BatchSink,
)

__all__ = [
    "PluginBase",
    "Tap",
    "Target",
    "streams",
    "Stream",
    "RESTStream",
    "GraphQLStream",
    "Sink",
    "RecordSink",
    "BatchSink",
]
