"""SDK for building Singer taps."""

from __future__ import annotations

from singer_sdk.streams.core import Stream
from singer_sdk.streams.graphql import GraphQLStream
from singer_sdk.streams.rest import RESTStream
from singer_sdk.streams.sql import SQLStream

__all__ = ["GraphQLStream", "RESTStream", "SQLStream", "Stream"]
