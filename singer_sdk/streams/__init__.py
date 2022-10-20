"""SDK for building Singer taps."""

from singer_sdk.streams.core import Stream
from singer_sdk.streams.graphql import GraphQLStream
from singer_sdk.streams.rest import RESTStream
from singer_sdk.streams.sql import SQLStream

__all__ = ["Stream", "GraphQLStream", "RESTStream", "SQLStream"]
