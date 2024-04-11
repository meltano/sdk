"""Sink classes for targets."""

from __future__ import annotations

from singer_sdk.sinks.batch import BatchSink
from singer_sdk.sinks.core import Sink
from singer_sdk.sinks.record import RecordSink
from singer_sdk.sinks.sql import SQLSink

__all__ = ["BatchSink", "RecordSink", "SQLSink", "Sink"]
