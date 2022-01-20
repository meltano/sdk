"""SDK for building singer-compliant Singer taps."""

from singer_sdk import streams
from singer_sdk.mapper_base import InlineMapper
from singer_sdk.plugin_base import PluginBase
from singer_sdk.sinks import BatchSink, RecordSink, Sink, SQLSink
from singer_sdk.streams import (
    GraphQLStream,
    RESTStream,
    SQLConnector,
    SQLStream,
    Stream,
)
from singer_sdk.tap_base import SQLTap, Tap
from singer_sdk.target_base import SQLTarget, Target

__all__ = [
    "BatchSink",
    "GraphQLStream",
    "InlineMapper",
    "PluginBase",
    "RecordSink",
    "RESTStream",
    "Sink",
    "SQLConnector",
    "SQLSink",
    "SQLStream",
    "SQLTap",
    "SQLTarget",
    "Stream",
    "streams",
    "Tap",
    "Target",
]
