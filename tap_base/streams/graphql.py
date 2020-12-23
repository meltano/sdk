"""Abstract base class for API-type streams."""

import abc
import jinja2
import requests

from datetime import datetime
from typing import Any, Dict, Iterable, Optional, Union

from singer.schema import Schema

from tap_base.streams.rest import RESTStreamBase

URLArgMap = Dict[str, Union[str, bool, int, datetime]]

DEFAULT_PAGE_SIZE = 1000


class GraphQLStreamBase(RESTStreamBase, metaclass=abc.ABCMeta):
    """Abstract base class for API-type streams."""

    graphql_query: Optional[Union[str, jinja2.Template]] = None
    url_suffix = ""

    def __init__(
        self,
        config: dict,
        state: Dict[str, Any],
        name: Optional[str] = None,
        schema: Optional[Union[Dict[str, Any], Schema]] = None,
    ):
        super().__init__(
            name=name,
            schema=schema,
            state=state,
            config=config,
            url_suffix="",  # use the base URL directly for GraphQL sources.
        )
        self._requests_session = requests.Session()

    def prepare_request(
        self, url, params=None, method="POST", json=None
    ) -> requests.PreparedRequest:
        self.logger.info("Preparing GraphQL API request...")
        if method != "POST":
            raise ValueError("Argument 'method' must be 'POST' for GraphQL streams.")
        if not self.graphql_query:
            raise ValueError("Missing value for 'graphql_query'.")
        if isinstance(self.graphql_query, jinja2.Template):
            query = self.graphql_query.render(**self.template_values)
        else:
            query = self.graphql_query
        query = "query { " + (" ".join([l.strip() for l in query.splitlines()])) + " }"
        self.logger.info(f"Attempting query:\n{query}")
        return super().prepare_request(
            url=url, params=params, method="POST", json={"query": query}
        )

    def parse_response(self, response) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        resp_json = response.json()
        for row in resp_json["data"][self.name]:
            yield row
