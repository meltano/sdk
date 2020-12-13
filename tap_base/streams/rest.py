"""Abstract base class for API-type streams."""

import abc
import json
import backoff
import logging
import requests
import sys

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
        config: dict,
        logger: logging.Logger,
        state: Dict[str, Any],
        name: Optional[str],
        schema: Optional[Union[Dict[str, Any], Schema]],
        url_pattern: Optional[str],
    ):
        super().__init__(
            name=name, schema=schema, state=state, logger=logger, config=config,
        )
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
    def url_encode(val: Union[str, datetime, bool, int, List[str]]) -> str:
        if isinstance(val, str):
            result = val.replace("/", "%2F")
        else:
            result = str(val)
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
    def request_get_with_backoff(self, url, params=None) -> requests.Response:
        params = params or {}

        request = self.prepare_request(url=url, params=params)
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
        logging.debug("Response received successfully.")
        return response

    def prepare_request(
        self, url, params=None, method="GET", data=None, json=None
    ) -> requests.PreparedRequest:
        request = requests.Request(
            method=method,
            url=url,
            params=params,
            headers=self.get_auth_header(),
            data=data,
            json=json,
        ).prepare()
        return request

    def request_paginated_get(self) -> Iterable[dict]:
        params = {"page": 1, "per_page": self._page_size}
        next_page = 1
        url = self.endpoint_url
        while next_page:
            params["page"] = int(next_page)
            resp = self.request_get_with_backoff(url, params)
            for row in self.parse_response(resp):
                yield row
            next_page = self.get_next_page(resp)

    def get_next_page(self, response):
        return response.headers.get("X-Next-Page", None)

    def parse_response(self, response) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        resp_json = response.json()
        if isinstance(resp_json, dict):
            yield resp_json
        else:
            for row in resp_json:
                yield row

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
