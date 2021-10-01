"""SDK for building singer-compliant Singer taps."""

from singer_sdk import streams
from singer_sdk.plugin_base import PluginBase
from singer_sdk.sinks import BatchSink, RecordSink, Sink
from singer_sdk.streams import GraphQLStream, RESTStream, Stream
from singer_sdk.tap_base import Tap
from singer_sdk.target_base import Target

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
