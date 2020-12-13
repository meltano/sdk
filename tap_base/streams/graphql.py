"""Abstract base class for API-type streams."""

import abc
import requests
import logging

from datetime import datetime
from typing import Any, Dict, Iterable, Optional, Union

from singer.schema import Schema

from tap_base.streams.rest import RESTStreamBase

URLArgMap = Dict[str, Union[str, bool, int, datetime]]

DEFAULT_PAGE_SIZE = 1000


class GraphQLStreamBase(RESTStreamBase, metaclass=abc.ABCMeta):
    """Abstract base class for API-type streams."""

    graphql_query: Optional[str] = None
    url_pattern = ""  # use the base URL directly for GraphQL sources.

    def __init__(
        self,
        config: dict,
        logger: logging.Logger,
        state: Dict[str, Any],
        name: Optional[str] = None,
        schema: Optional[Union[Dict[str, Any], Schema]] = None,
    ):
        super().__init__(
            name=name,
            schema=schema,
            state=state,
            logger=logger,
            config=config,
            url_pattern="",
        )
        self._requests_session = requests.Session()

    def prepare_request(
        self, url, params=None, method="POST", data=None, json=None
    ) -> requests.PreparedRequest:
        self.logger.info("Preparing GraphQL API request...")
        if method != "POST":
            raise ValueError("Argument 'method' must be 'POST' for GraphQL streams.")
        if all([data, self.graphql_query]) and data != self.graphql_query:
            raise ValueError(
                "Argument 'data' conflicts with property 'graphql_query'. "
                "Expected one or the other, but not both."
            )
        if not any([data, self.graphql_query]):
            raise ValueError(
                "Must specify either argument 'data' or property 'graphql_query'."
            )
        query = (
            data
            or "query {\n    " + ("\n    ".join(self.graphql_query.splitlines())) + "}"
        )
        self.logger.info(f"Attempting query:\n{query}")
        return super().prepare_request(
            url=url, params=params, method="POST", json={"query": query}
        )

    def parse_response(self, response) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        resp_json = response.json()
        for row in resp_json["data"][self.name]:
            yield row
