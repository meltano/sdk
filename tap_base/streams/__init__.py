"""tap-base library for building singer-compliant taps."""

from tap_base.streams.core import Stream
from tap_base.streams.rest import RESTStream
from tap_base.streams.database import DatabaseStream
from tap_base.streams.graphql import GraphQLStream


__all__ = [
    "Stream",
    "RESTStream",
    "DatabaseStream",
    "GraphQLStream",
]
