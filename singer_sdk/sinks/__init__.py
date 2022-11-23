"""Sink classes for targets."""

from singer_sdk.sinks.batch import BatchSink
from singer_sdk.sinks.core import Sink
from singer_sdk.sinks.record import RecordSink
from singer_sdk.sinks.sql import SQLSink

__all__ = ["BatchSink", "RecordSink", "Sink", "SQLSink"]
