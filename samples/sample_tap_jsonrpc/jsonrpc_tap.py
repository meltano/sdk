"""Sample tap for JSON-RPC API.

This tap demonstrates how to implement a Singer tap that connects to
a JSON-RPC API endpoint using the JSONRPCStream class.
"""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk.typing import (
    IntegerType,
    PropertiesList,
    Property,
    StringType,
)

from .jsonrpc_streams import InfoStream, ItemsStream


class SampleTapJsonRpc(Tap):
    """Sample tap for JSON-RPC API."""

    name = "sample-tap-jsonrpc"

    config_jsonschema = PropertiesList(
        Property(
            "endpoint_url",
            StringType,
            required=True,
            description="The URL for the JSON-RPC API endpoint",
        ),
        Property(
            "batch_size",
            IntegerType,
            default=10,
            description="Number of items to request in each batch",
        ),
    ).to_dict()

    def discover_streams(self) -> list:
        """Return a list of discovered streams."""
        return [
            InfoStream(tap=self),
            ItemsStream(tap=self),
        ]
