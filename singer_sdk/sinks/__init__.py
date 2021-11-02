"""Sink classes for targets."""

from singer_sdk.sinks.core import Sink
from singer_sdk.sinks.record import RecordSink
from singer_sdk.sinks.batch import BatchSink

__all__ = [
    "Sink",
    "RecordSink",
    "BatchSink",
]