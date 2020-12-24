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
    rest_method = "POST"

    def prepare_request_payload(self) -> Optional[dict]:
        """Prepare the data payload for the REST API request."""
        if isinstance(self.graphql_query, jinja2.Template):
            query = self.graphql_query.render(**self.template_values)
        else:
            query = self.graphql_query
        request_data = {
            "query": "query { "
            + (" ".join([l.strip() for l in query.splitlines()]))
            + " }"
        }
        self.logger.info(f"Attempting query:\n{query}")
        return request_data

    def parse_response(self, response) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        resp_json = response.json()
        for row in resp_json["data"][self.name]:
            yield row
