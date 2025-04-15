"""Abstract base class for API-type streams."""

from __future__ import annotations

import abc
import typing as t

from singer_sdk.helpers._classproperty import classproperty
from singer_sdk.streams.rest import RESTStream

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context

_TToken = t.TypeVar("_TToken")


class GraphQLStream(RESTStream, t.Generic[_TToken], metaclass=abc.ABCMeta):
    """Abstract base class for API-type streams.

    GraphQL streams inherit from the class `GraphQLStream`, which in turn inherits from
    the `RESTStream` class. GraphQL streams are very similar to REST API-based streams,
    but instead of specifying a `path` and `url_params`, developers override the
    GraphQL query text.
    """

    path = ""
    http_method = "POST"

    @classproperty
    def records_jsonpath(cls) -> str:  # type: ignore[override] # noqa: N805
        """Get the JSONPath expression to extract records from an API response.

        Returns:
            JSONPath expression string
        """
        return f"$.data.{cls.name}[*]"

    @property
    def query(self) -> str:
        """Set or return the GraphQL query string.

        Raises:
            NotImplementedError: If the derived class doesn't define this property.
        """
        msg = "GraphQLStream `query` is not defined."
        raise NotImplementedError(msg)

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
