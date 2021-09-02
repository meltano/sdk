"""SDK for building singer-compliant taps."""

from singer_sdk.streams.core import Stream
from singer_sdk.streams.rest import RESTStream
from singer_sdk.streams.graphql import GraphQLStream
from singer_sdk.streams.database import SQLStream


__all__ = [
    "Stream",
    "RESTStream",
    "GraphQLStream",
    "SQLStream",
]
