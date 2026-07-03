"""Abstract base class for API-type streams."""

from __future__ import annotations

import abc
import sys
import typing as t

from singer_sdk.exceptions import FatalAPIError
from singer_sdk.streams.rest import RESTStream

if sys.version_info >= (3, 12):
    from typing import override  # noqa: ICN003
else:
    from typing_extensions import override

if t.TYPE_CHECKING:
    import requests

    from singer_sdk.helpers.types import Context

_TToken = t.TypeVar("_TToken")


class GraphQLStream(RESTStream, abc.ABC, t.Generic[_TToken]):
    """Abstract base class for API-type streams.

    GraphQL streams inherit from the class `GraphQLStream`, which in turn inherits from
    the `RESTStream` class. GraphQL streams are very similar to REST API-based streams,
    but instead of specifying a `path` and `url_params`, developers override the
    GraphQL query text.
    """

    path = ""
    http_method = "POST"

    @property
    @override
    def records_jsonpath(self) -> str:
        """Get the JSONPath expression to extract records from an API response.

        Returns:
            JSONPath expression string.
        """
        return f"$.data.{self.name}[*]"

    @property
    def query(self) -> str:
        """Set or return the GraphQL query string.

        Raises:
            NotImplementedError: If the derived class doesn't define this property.
        """
        msg = "GraphQLStream `query` is not defined."
        raise NotImplementedError(msg)

    @override
    def prepare_request_payload(
        self,
        context: Context | None,
        next_page_token: _TToken | None,
    ) -> dict | None:
        """Prepare the data payload for the GraphQL API request.

        Developers generally should generally not need to override this method.
        Instead, developers set the payload by properly configuring the `query`
        attribute.

        Args:
            context: Stream partition or context dictionary.
            next_page_token: Token, page number or any request argument to request the
                next page of data.

        Returns:
            Dictionary with the body to use for the request.

        Raises:
            ValueError: If the `query` property is not set in the request body.
        """
        params = self.get_url_params(context, next_page_token)
        query = self.query

        if query is None:
            msg = "Graphql `query` property not set."  # type: ignore[unreachable]
            raise ValueError(msg)

        if not query.lstrip().startswith("query"):
            # Wrap text in "query { }" if not already wrapped
            query = "query { " + query + " }"

        query = query.lstrip()
        request_data = {
            "query": (" ".join([line.strip() for line in query.splitlines()])),
            "variables": params,
        }
        self.logger.debug("Attempting query:\n%s", query)
        return request_data

    @override
    def validate_response(self, response: requests.Response) -> None:
        """Validate the GraphQL response.

        GraphQL APIs typically return HTTP 200 even on failure, reporting errors
        in the response body under an ``errors`` key. This extends the parent
        status-code checks to surface those body-level errors.

        Args:
            response: A :class:`requests.Response` object.

        Raises:
            FatalAPIError: If the response body is not valid JSON, is not a
                JSON object, or reports GraphQL errors with no data.
        """
        super().validate_response(response)

        try:
            json_response = response.json()
        except ValueError as e:
            msg = "GraphQL response body is not valid JSON"
            raise FatalAPIError(msg) from e

        if not isinstance(json_response, dict):
            msg = "GraphQL response body must be a JSON object"
            raise FatalAPIError(msg)

        errors = json_response.get("errors")
        if not errors:
            return

        if isinstance(errors, list):
            messages = ", ".join(
                str(e.get("message", e)) if isinstance(e, dict) else str(e)
                for e in errors
            )
        else:
            messages = str(errors)

        msg = f"GraphQL errors in response: {messages}"

        # Partial success: data is present alongside errors. Records can still be
        # extracted, so warn rather than abort. Taps needing retriable handling
        # (e.g. RATE_LIMITED in extensions) should override this method.
        if json_response.get("data"):
            self.logger.warning(msg)
        else:
            raise FatalAPIError(msg)
