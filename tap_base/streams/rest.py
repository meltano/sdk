"""Abstract base class for API-type streams."""

import abc
import backoff
import logging
import jinja2
import requests

from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Union

from singer.schema import Schema

from tap_base.streams.core import TapStreamBase

URLArgMap = Dict[str, Union[str, bool, int, datetime]]

DEFAULT_PAGE_SIZE = 1000


class RESTStreamBase(TapStreamBase, metaclass=abc.ABCMeta):
    """Abstract base class for API-type streams."""

    _page_size: int = DEFAULT_PAGE_SIZE
    _requests_session: Optional[requests.Session]

    @property
    @abc.abstractmethod
    def site_url_base(self) -> str:
        """Return the base url, e.g. 'https://api.mysite.com/v3/'."""
        pass

    def __init__(
        self,
        config: dict,
        state: Dict[str, Any],
        name: Optional[str] = None,
        schema: Optional[Union[Dict[str, Any], Schema]] = None,
        url_suffix: Optional[str] = None,
    ):
        super().__init__(
            name=name, schema=schema, state=state, config=config,
        )
        if url_suffix:
            self.url_suffix = url_suffix
        self._requests_session = requests.Session()
        self._cached_auth_header: Optional[dict] = None

    @staticmethod
    def url_encode(val: Union[str, datetime, bool, int, List[str]]) -> str:
        if isinstance(val, str):
            result = val.replace("/", "%2F")
        else:
            result = str(val)
        return result

    def get_query_params(self) -> Union[List[URLArgMap], URLArgMap]:
        return [{}]

    def get_urls(self) -> List[str]:
        url_pattern = "".join([self.site_url_base, self.url_suffix or ""])
        result: List[str] = []
        param_list = self.get_query_params()
        if not isinstance(param_list, list):
            param_list = [param_list]
        for replacement_map in param_list:
            url = url_pattern
            for k, v in replacement_map.items():
                search_text = "".join(["{", k, "}"])
                if search_text in url:
                    url = url.replace(search_text, self.url_encode(v))
            self.logger.info(
                f"Tap '{self.name}' generated URL: {url} from param list {param_list} "
                f"and url_suffix '{self.url_suffix}'"
            )
            result.append(url)
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
            raise RuntimeError(
                "Error making request to API: GET {} [{} - {}]".format(
                    request.url, response.status_code, response.content
                )
            )
        logging.debug("Response received successfully.")
        return response

    def render(self, input: Union[str, jinja2.Template]) -> str:
        if isinstance(input, jinja2.Template):
            return str(input.render(**self.template_values))
        return str(input)

    def prepare_request(
        self, url, params=None, method="GET", json=None
    ) -> requests.PreparedRequest:
        if not self._cached_auth_header:
            self._cached_auth_header = self.get_auth_header()
        request = requests.Request(
            method=method,
            url=self.render(url),
            params=params,
            headers=self._cached_auth_header,
            json=json,
        ).prepare()
        return request

    def request_paginated_get(self) -> Iterable[dict]:
        params = {"page": 1, "per_page": self._page_size}
        next_page = 1
        for url in self.get_urls():
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
            yield row

    # Abstract methods:

    @abc.abstractmethod
    def get_auth_header(self) -> Dict[str, Any]:
        """Return an authorization header for REST API requests."""
        pass
