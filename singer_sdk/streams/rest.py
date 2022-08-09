"""Abstract base class for API-type streams."""

from __future__ import annotations

import abc
import copy
import logging
from datetime import datetime
from typing import Any, Callable, Generator, Generic, Iterable, TypeVar, Union
from urllib.parse import urlparse

import backoff
import requests
from singer.schema import Schema

from singer_sdk.authenticators import APIAuthenticatorBase, SimpleAuthenticator
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.plugin_base import PluginBase as TapBaseClass
from singer_sdk.streams.core import Stream

DEFAULT_PAGE_SIZE = 1000
DEFAULT_REQUEST_TIMEOUT = 300  # 5 minutes

_TToken = TypeVar("_TToken")
_T = TypeVar("_T")
_MaybeCallable = Union[_T, Callable[[], _T]]


class RESTStream(Stream, Generic[_TToken], metaclass=abc.ABCMeta):
    """Abstract base class for REST API streams."""

    _page_size: int = DEFAULT_PAGE_SIZE
    _requests_session: requests.Session | None
    rest_method = "GET"

    #: JSONPath expression to extract records from the API response.
    records_jsonpath: str = "$[*]"

    #: Response code reference for rate limit retries
    extra_retry_statuses: list[int] = [429]

    #: Optional JSONPath expression to extract a pagination token from the API response.
    #: Example: `"$.next_page"`
    next_page_token_jsonpath: str | None = None

    # Private constants. May not be supported in future releases:
    _LOG_REQUEST_METRICS: bool = True
    # Disabled by default for safety:
    _LOG_REQUEST_METRIC_URLS: bool = False

    @property
    @abc.abstractmethod
    def url_base(self) -> str:
        """Return the base url, e.g. ``https://api.mysite.com/v3/``."""
        pass

    def __init__(
        self,
        tap: TapBaseClass,
        name: str | None = None,
        schema: dict[str, Any] | Schema | None = None,
        path: str | None = None,
    ) -> None:
        """Initialize the REST stream.

        Args:
            tap: Singer Tap this stream belongs to.
            schema: JSON schema for records in this stream.
            name: Name of this stream.
            path: URL path for this entity stream.
        """
        super().__init__(name=name, schema=schema, tap=tap)
        if path:
            self.path = path
        self._http_headers: dict = {}
        self._requests_session = requests.Session()
        self._compiled_jsonpath = None
        self._next_page_token_compiled_jsonpath = None

    @staticmethod
    def _url_encode(val: str | datetime | bool | int | list[str]) -> str:
        """Encode the val argument as url-compatible string.

        Args:
            val: TODO

        Returns:
            TODO
        """
        if isinstance(val, str):
            result = val.replace("/", "%2F")
        else:
            result = str(val)
        return result

    def get_url(self, context: dict | None) -> str:
        """Get stream entity URL.

        Developers override this method to perform dynamic URL generation.

        Args:
            context: Stream partition or context dictionary.

        Returns:
            A URL, optionally targeted to a specific partition or context.
        """
        url = "".join([self.url_base, self.path or ""])
        vals = copy.copy(dict(self.config))
        vals.update(context or {})
        for k, v in vals.items():
            search_text = "".join(["{", k, "}"])
            if search_text in url:
                url = url.replace(search_text, self._url_encode(v))
        return url

    # HTTP Request functions

    @property
    def requests_session(self) -> requests.Session:
        """Get requests session.

        Returns:
            The `requests.Session`_ object for HTTP requests.

        .. _requests.Session:
            https://docs.python-requests.org/en/latest/api/#request-sessions
        """
        if not self._requests_session:
            self._requests_session = requests.Session()
        return self._requests_session

    def validate_response(self, response: requests.Response) -> None:
        """Validate HTTP response.

        Checks for error status codes and wether they are fatal or retriable.

        In case an error is deemed transient and can be safely retried, then this
        method should raise an :class:`singer_sdk.exceptions.RetriableAPIError`.
        By default this applies to 5xx error codes, along with values set in:
        :attr:`~singer_sdk.RESTStream.extra_retry_statuses`

        In case an error is unrecoverable raises a
        :class:`singer_sdk.exceptions.FatalAPIError`. By default, this applies to
        4xx errors, excluding values found in:
        :attr:`~singer_sdk.RESTStream.extra_retry_statuses`

        Tap developers are encouraged to override this method if their APIs use HTTP
        status codes in non-conventional ways, or if they communicate errors
        differently (e.g. in the response body).

        .. image:: ../images/200.png

        Args:
            response: A `requests.Response`_ object.

        Raises:
            FatalAPIError: If the request is not retriable.
            RetriableAPIError: If the request is retriable.

        .. _requests.Response:
            https://docs.python-requests.org/en/latest/api/#requests.Response
        """
        if (
            response.status_code in self.extra_retry_statuses
            or 500 <= response.status_code < 600
        ):
            msg = self.response_error_message(response)
            raise RetriableAPIError(msg, response)
        elif 400 <= response.status_code < 500:
            msg = self.response_error_message(response)
            raise FatalAPIError(msg)

    def response_error_message(self, response: requests.Response) -> str:
        """Build error message for invalid http statuses.

        WARNING - Override this method when the URL path may contain secrets or PII

        Args:
            response: A `requests.Response`_ object.

        Returns:
            str: The error message
        """
        full_path = urlparse(response.url).path or self.path
        if 400 <= response.status_code < 500:
            error_type = "Client"
        else:
            error_type = "Server"

        return (
            f"{response.status_code} {error_type} Error: "
            f"{response.reason} for path: {full_path}"
        )

    def request_decorator(self, func: Callable) -> Callable:
        """Instantiate a decorator for handling request failures.

        Uses a wait generator defined in `backoff_wait_generator` to
        determine backoff behaviour. Try limit is defined in
        `backoff_max_tries`, and will trigger the event defined in
        `backoff_handler` before retrying. Developers may override one or
        all of these methods to provide custom backoff or retry handling.

        Args:
            func: Function to decorate.

        Returns:
            A decorated method.
        """
        decorator: Callable = backoff.on_exception(
            self.backoff_wait_generator,
            (
                RetriableAPIError,
                requests.exceptions.ReadTimeout,
            ),
            max_tries=self.backoff_max_tries,
            on_backoff=self.backoff_handler,
        )(func)
        return decorator

    def _request(
        self, prepared_request: requests.PreparedRequest, context: dict | None
    ) -> requests.Response:
        """TODO.

        Args:
            prepared_request: TODO
            context: Stream partition or context dictionary.

        Returns:
            TODO
        """
        response = self.requests_session.send(prepared_request, timeout=self.timeout)
        if self._LOG_REQUEST_METRICS:
            extra_tags = {}
            if self._LOG_REQUEST_METRIC_URLS:
                extra_tags["url"] = prepared_request.path_url
            self._write_request_duration_log(
                endpoint=self.path,
                response=response,
                context=context,
                extra_tags=extra_tags,
            )
        self.validate_response(response)
        logging.debug("Response received successfully.")
        return response

    def get_url_params(
        self, context: dict | None, next_page_token: _TToken | None
    ) -> dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization.

        If paging is supported, developers may override with specific paging logic.

        Args:
            context: Stream partition or context dictionary.
            next_page_token: Token, page number or any request argument to request the
                next page of data.

        Returns:
            Dictionary of URL query parameters to use in the request.
        """
        return {}

    def prepare_request(
        self, context: dict | None, next_page_token: _TToken | None
    ) -> requests.PreparedRequest:
        """Prepare a request object.

        If partitioning is supported, the `context` object will contain the partition
        definitions. Pagination information can be parsed from `next_page_token` if
        `next_page_token` is not None.

        Args:
            context: Stream partition or context dictionary.
            next_page_token: Token, page number or any request argument to request the
                next page of data.

        Returns:
            Build a request with the stream's URL, path, query parameters,
            HTTP headers and authenticator.
        """
        http_method = self.rest_method
        url: str = self.get_url(context)
        params: dict = self.get_url_params(context, next_page_token)
        request_data = self.prepare_request_payload(context, next_page_token)
        headers = self.http_headers

        authenticator = self.authenticator
        if authenticator:
            headers.update(authenticator.auth_headers or {})
            params.update(authenticator.auth_params or {})

        request = self.requests_session.prepare_request(
            requests.Request(
                method=http_method,
                url=url,
                params=params,
                headers=headers,
                json=request_data,
            ),
        )
        return request

    def request_records(self, context: dict | None) -> Iterable[dict]:
        """Request records from REST endpoint(s), returning response records.

        If pagination is detected, pages will be recursed automatically.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            An item for every record in the response.

        Raises:
            RuntimeError: If a loop in pagination is detected. That is, when two
                consecutive pagination tokens are identical.
        """
        next_page_token: _TToken | None = None
        finished = False
        decorated_request = self.request_decorator(self._request)

        while not finished:
            prepared_request = self.prepare_request(
                context, next_page_token=next_page_token
            )
            resp = decorated_request(prepared_request, context)
            self.update_sync_costs(prepared_request, resp, context)
            yield from self.parse_response(resp)
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

    def update_sync_costs(
        self,
        request: requests.PreparedRequest,
        response: requests.Response,
        context: dict | None,
    ) -> dict[str, int]:
        """Update internal calculation of Sync costs.

        Args:
            request: the Request object that was just called.
            response: the `requests.Response` object
            context: the context passed to the call

        Returns:
            A dict of costs (for the single request) whose keys are
            the "cost domains". See `calculate_sync_cost` for details.
        """
        call_costs = self.calculate_sync_cost(request, response, context)
        self._sync_costs = {
            k: self._sync_costs.get(k, 0) + call_costs.get(k, 0)
            for k in call_costs.keys()
        }
        return self._sync_costs

    # Overridable:

    def calculate_sync_cost(
        self,
        request: requests.PreparedRequest,
        response: requests.Response,
        context: dict | None,
    ) -> dict[str, int]:
        """Calculate the cost of the last API call made.

        This method can optionally be implemented in streams to calculate
        the costs (in arbitrary units to be defined by the tap developer)
        associated with a single API/network call. The request and response objects
        are available in the callback, as well as the context.

        The method returns a dict where the keys are arbitrary cost dimensions,
        and the values the cost along each dimension for this one call. For
        instance: { "rest": 0, "graphql": 42 } for a call to github's graphql API.
        All keys should be present in the dict.

        This method can be overridden by tap streams. By default it won't do
        anything.

        Args:
            request: the API Request object that was just called.
            response: the `requests.Response` object
            context: the context passed to the call

        Returns:
            A dict of accumulated costs whose keys are the "cost domains".
        """
        return {}

    def prepare_request_payload(
        self, context: dict | None, next_page_token: _TToken | None
    ) -> dict | None:
        """Prepare the data payload for the REST API request.

        By default, no payload will be sent (return None).

        Developers may override this method if the API requires a custom payload along
        with the request. (This is generally not required for APIs which use the
        HTTP 'GET' method.)

        Args:
            context: Stream partition or context dictionary.
            next_page_token: Token, page number or any request argument to request the
                next page of data.

        Returns:
            Dictionary with the body to use for the request.
        """
        return None

    def get_next_page_token(
        self,
        response: requests.Response,
        previous_token: _TToken | None,
    ) -> _TToken | None:
        """Return token identifying next page or None if all records have been read.

        Args:
            response: A raw `requests.Response`_ object.
            previous_token: Previous pagination reference.

        Returns:
            Reference value to retrieve next page.

        .. _requests.Response:
            https://docs.python-requests.org/en/latest/api/#requests.Response
        """
        if self.next_page_token_jsonpath:
            all_matches = extract_jsonpath(
                self.next_page_token_jsonpath, response.json()
            )
            first_match = next(iter(all_matches), None)
            next_page_token = first_match
        else:
            next_page_token = response.headers.get("X-Next-Page", None)

        return next_page_token

    @property
    def http_headers(self) -> dict:
        """Return headers dict to be used for HTTP requests.

        If an authenticator is also specified, the authenticator's headers will be
        combined with `http_headers` when making HTTP requests.

        Returns:
            Dictionary of HTTP headers to use as a base for every request.
        """
        result = self._http_headers
        if "user_agent" in self.config:
            result["User-Agent"] = self.config.get("user_agent")
        return result

    @property
    def timeout(self) -> int:
        """Return the request timeout limit in seconds.

        The default timeout is 300 seconds, or as defined by DEFAULT_REQUEST_TIMEOUT.

        Returns:
            The request timeout limit as number of seconds.
        """
        return DEFAULT_REQUEST_TIMEOUT

    # Records iterator

    def get_records(self, context: dict | None) -> Iterable[dict[str, Any]]:
        """Return a generator of row-type dictionary objects.

        Each row emitted should be a dictionary of property names to their values.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            One item per (possibly processed) record in the API.
        """
        for record in self.request_records(context):
            transformed_record = self.post_process(record, context)
            if transformed_record is None:
                # Record filtered out during post_process()
                continue
            yield transformed_record

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows.

        Args:
            response: A raw `requests.Response`_ object.

        Yields:
            One item for every item found in the response.

        .. _requests.Response:
            https://docs.python-requests.org/en/latest/api/#requests.Response
        """
        yield from extract_jsonpath(self.records_jsonpath, input=response.json())

    # Abstract methods:

    @property
    def authenticator(self) -> APIAuthenticatorBase | None:
        """Return or set the authenticator for managing HTTP auth headers.

        If an authenticator is not specified, REST-based taps will simply pass
        `http_headers` as defined in the stream class.

        Returns:
            Authenticator instance that will be used to authenticate all outgoing
            requests.
        """
        return SimpleAuthenticator(stream=self)

    def backoff_wait_generator(self) -> Callable[..., Generator[int, Any, None]]:
        """The wait generator used by the backoff decorator on request failure.

        See for options:
        https://github.com/litl/backoff/blob/master/backoff/_wait_gen.py

        And see for examples: `Code Samples <../code_samples.html#custom-backoff>`_

        Returns:
            The wait generator
        """
        return backoff.expo(factor=2)  # type: ignore # ignore 'Returning Any'

    def backoff_max_tries(self) -> _MaybeCallable[int] | None:
        """The number of attempts before giving up when retrying requests.

        Can be an integer, a zero-argument callable that returns an integer,
        or ``None`` to retry indefinitely.

        Returns:
            int | Callable[[], int] | None: Number of max retries, callable or
            ``None``.
        """
        return 5

    def backoff_handler(self, details: dict) -> None:
        """Adds additional behaviour prior to retry.

        By default will log out backoff details, developers can override
        to extend or change this behaviour.

        Args:
            details: backoff invocation details
                https://github.com/litl/backoff#event-handlers
        """
        logging.error(
            "Backing off {wait:0.1f} seconds after {tries} tries "
            "calling function {target} with args {args} and kwargs "
            "{kwargs}".format(**details)
        )

    def backoff_runtime(
        self, *, value: Callable[[Any], int]
    ) -> Generator[int, None, None]:
        """Optional backoff wait generator that can replace the default `backoff.expo`.

        It is based on parsing the thrown exception of the decorated method, making it
        possible for response values to be in scope.

        Args:
            value: a callable which takes as input the decorated
                function's thrown exception and determines how
                long to wait.

        Yields:
            The thrown exception
        """
        exception = yield  # type: ignore[misc]
        while True:
            exception = yield value(exception)
