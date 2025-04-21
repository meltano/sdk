"""Abstract base class for JSON-RPC API streams."""

from __future__ import annotations

import abc
import decimal
import json
import typing as t
import uuid

from singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from singer_sdk.helpers._classproperty import classproperty
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams.rest import _HTTPStream

if t.TYPE_CHECKING:
    import requests

    from singer_sdk.helpers.types import Context
    from singer_sdk.singerlib import Schema
    from singer_sdk.tap_base import Tap

_TToken = t.TypeVar("_TToken")

# Constants for JSON-RPC error code ranges
SERVER_ERROR_MIN = -32099
SERVER_ERROR_MAX = -32000


class JSONRPCStream(_HTTPStream, t.Generic[_TToken], metaclass=abc.ABCMeta):
    """Abstract base class for JSON-RPC API streams.

    This class implements the JSON-RPC 2.0 protocol for Singer streams.
    JSON-RPC is a stateless, light-weight remote procedure call (RPC) protocol
    that uses JSON as its data format.

    Reference: https://www.jsonrpc.org/specification
    """

    # Default to JSON-RPC 2.0
    jsonrpc_version: str = "2.0"

    # Force POST method for JSON-RPC
    _http_method: str = "POST"

    # Always use JSON for the payload
    payload_as_json: bool = True

    @classproperty
    def records_jsonpath(cls) -> str:  # type: ignore[override]  # noqa: N805
        """Get the JSONPath expression to extract records from an API response.

        By default, this looks for an array in the 'result' field or in 'result.data'.
        Override this property if your JSON-RPC API uses a different structure.

        Returns:
            JSONPath expression string
        """
        return "$.result[*]"

    def __init__(
        self,
        tap: Tap,
        name: str | None = None,
        schema: dict[str, t.Any] | Schema | None = None,
        path: str | None = None,
    ) -> None:
        """Initialize the JSON-RPC stream.

        Args:
            tap: Singer Tap this stream belongs to.
            schema: JSON schema for records in this stream.
            name: Name of this stream.
            path: URL path for this entity stream.
        """
        # JSON-RPC always uses POST, so we override http_method
        super().__init__(tap, name, schema, path, http_method="POST")

    @property
    @abc.abstractmethod
    def method(self) -> str:
        """The JSON-RPC method name to call.

        Each stream must implement this property to define which
        remote method will be invoked.

        Returns:
            The JSON-RPC method name.
        """
        ...

    @staticmethod
    def get_method_parameters(
        context: Context | None = None,  # noqa: ARG004
        next_page_token: _TToken | None = None,
    ) -> dict[str, t.Any]:
        """Return a dictionary of parameters to be passed to the JSON-RPC method.

        Args:
            context: Stream partition or context dictionary.
            next_page_token: Token, page number or any request argument to request the
                next page of data.

        Returns:
            A dictionary of parameters to pass in the JSON-RPC request.
        """
        params: dict[str, t.Any] = {}

        # Add pagination parameters if available
        if next_page_token:
            if isinstance(next_page_token, dict):
                params.update(next_page_token)
            else:
                params["page"] = next_page_token

        return params

    def prepare_request_payload(
        self,
        context: Context | None,
        next_page_token: _TToken | None,
    ) -> dict[str, t.Any]:
        """Prepare the data payload for the JSON-RPC request.

        This method builds a proper JSON-RPC 2.0 request payload.

        Args:
            context: Stream partition or context dictionary.
            next_page_token: Token, page number or any request argument to request the
                next page of data.

        Returns:
            A valid JSON-RPC request object.
        """
        params = self.get_method_parameters(context, next_page_token)

        # Build standard JSON-RPC 2.0 request object
        payload = {
            "jsonrpc": self.jsonrpc_version,
            "method": self.method,
            "id": str(uuid.uuid4()),
        }

        # Only add params if we have any
        if params:
            payload["params"] = params

        return payload

    def extract_records(self, response_json: dict) -> t.Iterable[dict]:
        """Extract records from the response using the configured JSONPath expression.

        Args:
            response_json: The JSON response from the API

        Yields:
            Records extracted from the response.
        """
        # Special handling for empty object results
        if "result" in response_json:
            result = response_json["result"]

            # Handle empty dict
            if isinstance(result, dict) and not result:
                # Empty dict in the result field should be treated as a single record
                yield {}
                return

            # Handle null/None value
            if result is None:
                # Null result treated as a single record
                yield {"result": None}
                return

            # Handle nested data structures
            if isinstance(result, dict):
                # Check for common nested arrays patterns
                for key in ["data", "items"]:
                    if key in result and isinstance(result[key], list):
                        yield from result[key]
                        return

        # Use JSONPath for all other cases
        yield from extract_jsonpath(
            self.records_jsonpath,
            input=response_json,
        )

    def parse_response(self, response: requests.Response) -> t.Iterable[dict]:
        """Parse the response and return an iterator of result records.

        This method handles standard JSON-RPC responses, including error objects.

        Args:
            response: A raw :class:`requests.Response`

        Yields:
            One item for every item found in the 'result' field of the response.

        Raises:
            FatalAPIError: If the response contains a JSON-RPC error.
            RetriableAPIError: If the response contains a retriable JSON-RPC error.
        """
        json_response = response.json(parse_float=decimal.Decimal)

        # Handle JSON-RPC error objects
        if "error" in json_response:
            error = json_response["error"]
            error_code = error.get("code", 0)
            error_message = error.get("message", "Unknown JSON-RPC error")
            error_data = error.get("data", {})

            # Determine if this is a retriable error based on the JSON-RPC spec
            # According to the spec, server-side errors are in the range -32000 to
            # -32099
            if SERVER_ERROR_MIN <= error_code <= SERVER_ERROR_MAX:
                # Server-side errors are generally retriable
                error_msg = f"JSON-RPC Server Error {error_code}: {error_message}"
                raise RetriableAPIError(
                    error_msg,
                    response,
                )
            else:
                # Client errors and other errors are generally fatal
                error_msg = (
                    f"JSON-RPC Error {error_code}: {error_message} - {error_data}"
                )
                raise FatalAPIError(error_msg)

        # Extract and yield records from the result
        if "result" not in json_response:
            self.logger.warning("No 'result' field found in JSON-RPC response")
            return

        # Use jsonpath to extract records from the response
        yield from self.extract_records(json_response)

    def validate_response(self, response: requests.Response) -> None:
        """Validate HTTP response.

        In addition to HTTP status validation from the parent class,
        this method will try to detect JSON-RPC specific error responses.

        Args:
            response: A :class:`requests.Response` object.

        Raises:
            FatalAPIError: If the request is not retriable.
            RetriableAPIError: If the request is retriable.
        """
        # First validate the HTTP response
        super().validate_response(response)

        # Validate content type
        content_type = response.headers.get("Content-Type", "")
        if "application/json" not in content_type and response.content:
            error_msg = (
                f"Invalid Content-Type: {content_type}. Expected application/json."
            )
            raise FatalAPIError(error_msg)

        # Try parsing JSON to check for JSON-RPC errors early
        # More detailed error handling happens in parse_response
        try:
            json_response = response.json()

            # Check if response is a JSON-RPC error
            if "error" in json_response:
                error = json_response["error"]
                if isinstance(error, dict) and "code" in error:
                    code = error["code"]
                    # Server-specific errors (between -32000 and -32099) are retriable
                    if SERVER_ERROR_MIN <= code <= SERVER_ERROR_MAX:
                        error_msg = (
                            f"JSON-RPC Server Error {code}: "
                            f"{error.get('message', 'No message')}"
                        )
                        raise RetriableAPIError(
                            error_msg,
                            response,
                        )
                    # Other error codes are fatal client errors
                    error_msg = (
                        f"JSON-RPC Error {code}: {error.get('message', 'No message')}"
                    )
                    raise FatalAPIError(
                        error_msg,
                        response,
                    )
        except (json.JSONDecodeError, ValueError):
            # Will be handled when we actually parse the response in detail
            pass
