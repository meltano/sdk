"""Abstract base class for API-type streams."""

import abc
import backoff
import requests
import logging

from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Union

from singer.schema import Schema

from tap_base.streams.core import TapStreamBase

URLArgMap = Dict[str, Union[str, bool, int, datetime]]

DEFAULT_PAGE_SIZE = 1000


class RESTStreamBase(TapStreamBase, metaclass=abc.ABCMeta):
    """Abstract base class for API-type streams."""

    _url_pattern: str
    _url_args: URLArgMap = {}
    _page_size: int = DEFAULT_PAGE_SIZE
    _requests_session: Optional[requests.Session]

    def __init__(
        self,
        tap_stream_id: str,
        schema: Union[Dict[str, Any], Schema],
        state: Dict[str, Any],
        logger: logging.Logger,
        config: dict,
        url_pattern: str,
    ):
        super().__init__(tap_stream_id, schema, state, logger, config)
        self._url_pattern = url_pattern
        self._requests_session = requests.Session()

    @abc.abstractproperty
    def site_url_base(self) -> str:
        """Return the base url, e.g. 'https://api.mysite.com/v3/'."""
        pass

    @property
    def endpoint_url(self) -> str:
        return self.get_url()

    @staticmethod
    def url_encode(val: Union[str, datetime, List[str]]) -> str:
        # TODO: Add escape logic
        result = str(val)
        result = result.replace("/", "%2F")
        return result

    def get_url(self, url_suffix: str = None, extra_url_args: URLArgMap = None) -> str:
        result = "".join([self.site_url_base, self._url_pattern, url_suffix or ""])
        replacement_map = extra_url_args or {}
        for k, v in replacement_map.items():
            search_text = "".join(["{", k, "}"])
            if search_text in self._url_pattern:
                result = result.replace(search_text, self.url_encode(v))
        return result

    @property
    def requests_session(self) -> requests.Session:
        if not self._requests_session:
            self._requests_session = requests.Session()
        return self._requests_session

    @backoff.on_exception(
        backoff.expo,
        (requests.exceptions.RequestException),
        max_tries=5,
        giveup=lambda e: e.response is not None
        and 400 <= e.response.status_code < 500,  # pylint: disable=line-too-long
        factor=2,
    )
    def request_get_with_backoff(self, url, params=None):
        params = params or {}

        request = requests.Request(
            "GET", url, params=params, headers=self.get_auth_header()
        ).prepare()
        self.logger.info("Calling GET: {}".format(request.url))
        response = self.requests_session.send(request)

        if response.status_code in [401, 403]:
            self.logger.info("Skipping request to {}".format(request.url))
            self.logger.info(
                "Reason: {} - {}".format(response.status_code, response.content)
            )
            raise RuntimeError(
                "Requested resource was unauthorized, forbidden, or not found."
            )
        elif response.status_code >= 400:
            self.logger.critical(
                "Error making request to API: GET {} [{} - {}]".format(
                    request.url, response.status_code, response.content
                )
            )
            self.fatal()
        return response

    def request_paginated_get(self) -> Iterable[dict]:
        params = {"page": 1, "per_page": self._page_size}
        next_page = 1
        url = self.endpoint_url
        while next_page:
            params["page"] = int(next_page)
            resp = self.request_get_with_backoff(url, params)
            resp_json = resp.json()
            if isinstance(resp_json, dict):
                yield resp_json
            else:
                for row in resp_json:
                    yield row
            next_page = resp.headers.get("X-Next-Page", None)

    def get_row_generator(self) -> Iterable[dict]:
        """Return a generator of row-type dictionary objects."""
        for row in self.request_paginated_get():
            yield self.post_process(row)

    # Abstract methods:

    @abc.abstractmethod
    def get_auth_header(self) -> Dict[str, Any]:
        """Return an authorization header for REST API requests."""
        pass

    @abc.abstractmethod
    def post_process(self, row: dict) -> dict:
        """Transform raw data from HTTP GET into the expected property values."""
        return row
