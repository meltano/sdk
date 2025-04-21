"""Stream classes for the JSON-RPC API.

This module contains simplified stream implementations for JSON-RPC:
- InfoStream: Basic service information
- ItemsStream: Paginated items from the service
"""

from __future__ import annotations

import abc
import typing as t
import uuid
from datetime import datetime, timezone

if t.TYPE_CHECKING:
    import requests

from singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from singer_sdk.pagination import BaseAPIPaginator
from singer_sdk.streams.jsonrpc import JSONRPCStream
from singer_sdk.typing import (
    ArrayType,
    DateTimeType,
    IntegerType,
    PropertiesList,
    Property,
    StringType,
)


class BaseJsonRpcStream(JSONRPCStream):
    """Base class for JSON-RPC streams."""

    #: Set of HTTP status codes that should trigger a retry
    RETRY_STATUS_CODES: t.ClassVar[set[int]] = {429, 500, 502, 503, 504}

    #: HTTP status code for successful responses
    HTTP_OK: t.ClassVar[int] = 200

    url_base = None
    records_jsonpath = "$[*]"  # This will be overridden in most cases

    @property
    @abc.abstractmethod
    def method(self) -> str:
        """Define the JSON-RPC method to call.

        Returns:
            The JSON-RPC method name.
        """
        error_message = "Subclasses must implement the 'method' property"
        raise NotImplementedError(error_message)

    def validate_response(self, response: requests.Response) -> None:
        """Validate HTTP response from the JSON-RPC server.

        Args:
            response: HTTP response to validate.

        Raises:
            RetriableAPIError: If there is a retriable error.
            FatalAPIError: If there is a non-retryable error.
        """
        if response.status_code in self.RETRY_STATUS_CODES:
            msg = f"Retryable HTTP error: {response.status_code} - {response.text}"
            self.logger.warning(msg)
            raise RetriableAPIError(msg, response.status_code)

        if response.status_code != self.HTTP_OK:
            msg = f"HTTP error: {response.status_code} - {response.text}"
            self.logger.error(msg)
            raise FatalAPIError(msg)

        try:
            response_json = response.json()
        except Exception as e:
            msg = f"Invalid JSON response: {response.text[:1000]}..."
            self.logger.exception(msg)
            error_msg = "Invalid JSON response"
            raise FatalAPIError(error_msg) from e

        if "error" in response_json:
            error = response_json["error"]
            error_code = error.get("code")
            error_message = error.get("message", "Unknown error")
            error_data = error.get("data")

            # Determine which JSON-RPC errors are retriable
            retryable_codes = {-32603, -32000}  # Internal error, Server error

            if error_code in retryable_codes:
                msg = f"Retryable JSON-RPC error: {error_code} - {error_message}"
                if error_data:
                    self.logger.warning("%s - Additional data: %s", msg, error_data)
                else:
                    self.logger.warning(msg)
                raise RetriableAPIError(msg, error_code)

            # Other errors are fatal
            msg = f"JSON-RPC error: {error_code} - {error_message}"
            if error_data:
                self.logger.error("%s - Additional data: %s", msg, error_data)
            else:
                self.logger.error(msg)
            raise FatalAPIError(msg)


class InfoStream(BaseJsonRpcStream):
    """Stream for retrieving service information."""

    name = "service_info"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    replication_key = None

    schema = PropertiesList(
        Property("id", StringType, description="Service ID"),
        Property("name", StringType, description="Service name"),
        Property("version", StringType, description="Service version"),
        Property("features", ArrayType(StringType), description="Supported features"),
        Property("retrieved_at", DateTimeType, description="Retrieval timestamp"),
    ).to_dict()

    @property
    def method(self) -> str:
        """Define the JSON-RPC method to call."""
        return "get_service_info"

    def prepare_request_payload(self, _context: dict | None = None) -> dict:
        """Prepare the JSON-RPC request payload.

        Args:
            _context: The stream context (unused).

        Returns:
            The JSON-RPC request payload.
        """
        return {
            "jsonrpc": "2.0",
            "method": self.method,
            "id": str(uuid.uuid4()),
            # No params needed for this method
        }

    @staticmethod
    def parse_response(response: requests.Response) -> t.Iterable[dict]:
        """Parse the response and return an iterator of result records.

        Args:
            response: The HTTP response from the API.

        Yields:
            Each record from the source.
        """
        data = response.json()
        result = data.get("result", {})

        # Add timestamp to the result using datetime with timezone
        result["retrieved_at"] = datetime.now(timezone.utc).isoformat()

        yield result


class ItemsPaginator(BaseAPIPaginator):
    """Paginator for paginated JSON-RPC responses."""

    def __init__(self, start_value: int | None = None):
        """Initialize the paginator.

        Args:
            start_value: The initial value (page number).
        """
        super().__init__(start_value)
        self.current_page = start_value or 1
        self.total_pages = None

    def get_next(self, response: requests.Response) -> int | None:
        """Get the next page value from the response.

        Args:
            response: The HTTP response from the API.

        Returns:
            The next page number or None if no more pages.
        """
        data = response.json()
        result = data.get("result", {})

        if isinstance(result, dict):
            self.total_pages = result.get("total_pages", 1)
            if self.current_page >= self.total_pages:
                return None
            self.current_page += 1
            return self.current_page

        return None


class ItemsStream(BaseJsonRpcStream):
    """Stream for retrieving paginated items."""

    name = "items"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    replication_key = None

    schema = PropertiesList(
        Property("id", IntegerType, description="Item ID"),
        Property("name", StringType, description="Item name"),
        Property("description", StringType, description="Item description"),
        Property("category", StringType, description="Item category"),
        Property("price", IntegerType, description="Item price"),
        Property("created_at", DateTimeType, description="Creation timestamp"),
    ).to_dict()

    @property
    def method(self) -> str:
        """Define the JSON-RPC method to call."""
        return "get_items"

    @staticmethod
    def get_new_paginator() -> BaseAPIPaginator:
        """Create a new pagination helper instance.

        Returns:
            A paginator instance.
        """
        return ItemsPaginator(start_value=1)

    @staticmethod
    def get_url_params(
        _context: dict | None = None,
        _next_page_token: int | None = None,
    ) -> dict | None:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            _context: The stream context (unused).
            _next_page_token: The next page token (unused).

        Returns:
            A dictionary of URL query parameters.
        """
        # Not used in JSON-RPC, but required by the parent class
        return {}

    def prepare_request_payload(
        self, _context: dict | None = None, next_page_token: int | None = None
    ) -> dict:
        """Prepare the JSON-RPC request payload.

        Args:
            _context: The stream context (unused).
            next_page_token: The next page token.

        Returns:
            The JSON-RPC request payload.
        """
        # Add pagination parameters
        page = next_page_token if next_page_token else 1
        batch_size = self.config.get("batch_size", 10)

        params = {"page": page, "limit": batch_size}

        return {
            "jsonrpc": "2.0",
            "method": self.method,
            "params": params,
            "id": str(uuid.uuid4()),
        }

    @staticmethod
    def parse_response(response: requests.Response) -> t.Iterable[dict]:
        """Parse the response and return an iterator of result records.

        Args:
            response: The HTTP response from the API.

        Yields:
            Each record from the source.
        """
        data = response.json()
        result = data.get("result", {})

        if isinstance(result, dict) and "items" in result:
            yield from result["items"]
