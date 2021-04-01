"""Abstract base class for API-type streams."""

import abc
import backoff
import copy
import logging
import requests

from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Union

from singer.schema import Schema

from singer_sdk.authenticators import APIAuthenticatorBase, SimpleAuthenticator
from singer_sdk.plugin_base import PluginBase as TapBaseClass
from singer_sdk.streams.core import Stream

DEFAULT_PAGE_SIZE = 1000


class RESTStream(Stream, metaclass=abc.ABCMeta):
    """Abstract base class for API-type streams."""

    _page_size: int = DEFAULT_PAGE_SIZE
    _requests_session: Optional[requests.Session]
    rest_method = "GET"

    @property
    @abc.abstractmethod
    def url_base(self) -> str:
        """Return the base url, e.g. 'https://api.mysite.com/v3/'."""
        pass

    def __init__(
        self,
        tap: TapBaseClass,
        name: Optional[str] = None,
        schema: Optional[Union[Dict[str, Any], Schema]] = None,
        path: Optional[str] = None,
    ):
        """Initialize the REST stream."""
        super().__init__(name=name, schema=schema, tap=tap)
        if path:
            self.path = path
        self._http_headers: dict = {}
        self._requests_session = requests.Session()

    @staticmethod
    def _url_encode(val: Union[str, datetime, bool, int, List[str]]) -> str:
        """Encode the val argument as url-compatible string."""
        if isinstance(val, str):
            result = val.replace("/", "%2F")
        else:
            result = str(val)
        return result

    def get_url(self, partition: Optional[dict]) -> str:
        """Return a URL, optionally targeted to a specific partition.

        Developers override this method to perform dynamic URL generation.
        """
        url_pattern = "".join([self.url_base, self.path or ""])
        params = copy.deepcopy(dict(self.config))
        if partition:
            params.update(partition)
        url = url_pattern
        for k, v in params.items():
            search_text = "".join(["{", k, "}"])
            if search_text in url:
                url = url.replace(search_text, self._url_encode(v))
        return url

    # HTTP Request functions

    @property
    def requests_session(self) -> requests.Session:
        """Return the session object for HTTP requests."""
        if not self._requests_session:
            self._requests_session = requests.Session()
        return self._requests_session

    @backoff.on_exception(
        backoff.expo,
        (requests.exceptions.RequestException),
        max_tries=5,
        giveup=lambda e: e.response is not None and 400 <= e.response.status_code < 500,
        factor=2,
    )
    def _request_with_backoff(self, prepared_request) -> requests.Response:
        response = self.requests_session.send(prepared_request)
        if response.status_code in [401, 403]:
            self.logger.info("Skipping request to {}".format(prepared_request.url))
            self.logger.info(f"Reason: {response.status_code} - {response.content}")
            raise RuntimeError(
                "Requested resource was unauthorized, forbidden, or not found."
            )
        elif response.status_code >= 400:
            raise RuntimeError(
                f"Error making request to API: {prepared_request.url} "
                f"[{response.status_code} - {response.content}]".replace("\\n", "\n")
            )
        logging.debug("Response received successfully.")
        return response

    def get_url_params(
        self, partition: Optional[dict], next_page_token: Optional[Any] = None
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization.

        If paging is supported, developers may override with specific paging logic.
        """
        return {}

    def prepare_request(
        self, partition: Optional[dict], next_page_token: Optional[Any] = None
    ) -> requests.PreparedRequest:
        """Preopare a request object.

        If partitioning is supported, the `partition` object will contain the partition
        definitions. Pagination information can be parsed from `next_page_token` if
        `next_page_token` is not None.
        """
        http_method = self.rest_method
        url: str = self.get_url(partition)
        params: dict = self.get_url_params(partition, next_page_token)
        request_data = self.prepare_request_payload(partition, next_page_token)
        headers = self.http_headers

        authenticator = self.authenticator
        if authenticator:
            headers.update(authenticator.auth_headers or {})

        request = self.requests_session.prepare_request(
            requests.Request(
                method=http_method,
                url=url,
                params=params,
                headers=headers,
                json=request_data,
            )
        )
        return request

    def request_records(self, partition: Optional[dict]) -> Iterable[dict]:
        """Request records from REST endpoint(s), returning response records.

        If pagination is detected, pages will be recursed automatically.
        """
        next_page_token: Any = None
        finished = False
        while not finished:
            prepared_request = self.prepare_request(
                partition, next_page_token=next_page_token
            )
            resp = self._request_with_backoff(prepared_request)
            for row in self.parse_response(resp):
                yield row
            previous_token = copy.deepcopy(next_page_token)
            next_page_token = self.get_next_page_token(
                response=resp, previous_token=previous_token
            )
            if next_page_token and next_page_token == previous_token:
                raise RuntimeError(
                    f"Loop detected in pagination. "
                    f"Pagination token {next_page_token} is identical to prior token."
                )
            # Cycle until get_next_page_token() no longer returns a value
            finished = not next_page_token

    # Overridable:

    def prepare_request_payload(
        self, partition: Optional[dict], next_page_token: Optional[Any] = None
    ) -> Optional[dict]:
        """Prepare the data payload for the REST API request.

        By default, no payload will be sent (return None).
        """
        return None

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any] = None
    ) -> Any:
        """Return token identifying next page or None if all records have been read."""
        next_page_token = response.headers.get("X-Next-Page", None)
        if next_page_token:
            self.logger.info(f"Next page token retrieved: {next_page_token}")
        return next_page_token

    @property
    def http_headers(self) -> dict:
        """Return headers dict to be used for HTTP requests."""
        result = self._http_headers
        if "user_agent" in self.config:
            result["User-Agent"] = self.config.get("user_agent")
        return result

    # Records iterator

    def get_records(self, partition: Optional[dict]) -> Iterable[Dict[str, Any]]:
        """Return a generator of row-type dictionary objects.

        Each row emitted should be a dictionary of property names to their values.
        """
        for row in self.request_records(partition):
            row = self.post_process(row, partition)
            yield row

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        resp_json = response.json()
        if isinstance(resp_json, dict):
            yield resp_json
        else:
            for row in resp_json:
                yield row

    # Abstract methods:

    @property
    def authenticator(self) -> Optional[APIAuthenticatorBase]:
        """Return an authorization header for REST API requests."""
        return SimpleAuthenticator(stream=self)
