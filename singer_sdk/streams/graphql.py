"""Abstract base class for API-type streams."""

import abc
from typing import Any, Optional

from singer_sdk.helpers._classproperty import classproperty
from singer_sdk.streams.rest import RESTStream


class GraphQLStream(RESTStream, metaclass=abc.ABCMeta):
    """Abstract base class for API-type streams.

    GraphQL streams inherit from the class `GraphQLStream`, which in turn inherits from
    the `RESTStream` class. GraphQL streams are very similar to REST API-based streams,
    but instead of specifying a `path` and `url_params`, developers override the
    GraphQL query text.
    """

    path = ""
    rest_method = "POST"

    @classproperty
    def records_jsonpath(cls) -> str:  # type: ignore  # OK: str vs @classproperty
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
        raise NotImplementedError("GraphQLStream `query` is not defined.")

    def prepare_request_payload(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Optional[dict]:
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
        if self.query is None:
            raise ValueError("Graphql `query` property not set.")
        else:
            query = self.query
        if not query.lstrip().startswith("query"):
            # Wrap text in "query { }" if not already wrapped
            query = "query { " + query + " }"
        query = query.lstrip()
        request_data = {
            "query": (" ".join([line.strip() for line in query.splitlines()])),
            "variables": params,
        }
        self.logger.debug(f"Attempting query:\n{query}")
        return request_data
