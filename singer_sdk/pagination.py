"""Generic paginator classes."""

from __future__ import annotations

import sys
import typing as t
from abc import ABCMeta, abstractmethod
from urllib.parse import ParseResult, urlparse

from singer_sdk.helpers.jsonpath import extract_jsonpath

if sys.version_info < (3, 12):
    from typing_extensions import override
else:
    from typing import override  # noqa: ICN003

if t.TYPE_CHECKING:
    import requests

T = t.TypeVar("T")
TPageToken = t.TypeVar("TPageToken")


def first(iterable: t.Iterable[T]) -> T:
    """Return the first element of an iterable or raise an exception.

    Args:
        iterable: An iterable.

    Returns:
        The first element of the iterable.

    >>> first("ABC")
    'A'
    """
    return next(iter(iterable))


class BaseAPIPaginator(t.Generic[TPageToken], metaclass=ABCMeta):
    """An API paginator object."""

    def __init__(self, start_value: TPageToken) -> None:
        """Create a new paginator.

        Args:
            start_value: Initial value.
        """
        self._value: TPageToken = start_value
        self._page_count = 0
        self._finished = False
        self._last_seen_record: dict | None = None

    @property
    def current_value(self) -> TPageToken:
        """Get the current pagination value.

        Returns:
            Current page value.
        """
        return self._value

    @property
    def finished(self) -> bool:
        """Get a flag that indicates if the last page of data has been reached.

        Returns:
            True if there are no more pages.
        """
        return self._finished

    @property
    def count(self) -> int:
        """Count the number of pages traversed so far.

        Returns:
            Number of pages.
        """
        return self._page_count

    def __str__(self) -> str:
        """Stringify this object.

        Returns:
            String representation.
        """
        return f"{self.__class__.__name__}<{self.current_value}>"

    def __repr__(self) -> str:
        """Stringify this object.

        Returns:
            String representation.
        """
        return str(self)

    def advance(self, response: requests.Response) -> None:
        """Get a new page value and advance the current one.

        Args:
            response: API response object.

        Raises:
            RuntimeError: If a loop in pagination is detected. That is, when two
                consecutive pagination tokens are identical.
        """
        self._page_count += 1

        if not self.has_more(response):
            self._finished = True
            return

        new_value = self.get_next(response)

        if new_value and new_value == self._value:
            msg = (
                f"Loop detected in pagination. Pagination token {new_value} is "
                "identical to prior token."
            )
            raise RuntimeError(msg)

        # Stop if new value None, empty string, 0, etc.
        if not new_value:
            self._finished = True
        else:
            self._value = new_value

    def has_more(self, response: requests.Response) -> bool:  # noqa: ARG002, PLR6301
        """Override this method to check if the endpoint has any pages left.

        Args:
            response: API response object.

        Returns:
            Boolean flag used to indicate if the endpoint has more pages.
        """
        return True

    @abstractmethod
    def get_next(self, response: requests.Response) -> TPageToken | None:
        """Get the next pagination token or index from the API response.

        Args:
            response: API response object.

        Returns:
            The next page token or index. Return `None` from this method to indicate
                the end of pagination.
        """
        ...


class SinglePagePaginator(BaseAPIPaginator[None]):
    """A paginator that works with single-page endpoints."""

    def __init__(self, *args: t.Any, **kwargs: t.Any) -> None:
        """Create a new paginator.

        Args:
            args: Paginator positional arguments for base class.
            kwargs: Paginator keyword arguments for base class.
        """
        super().__init__(None, *args, **kwargs)

    @override
    def get_next(self, response: requests.Response) -> None:
        """Always return None to indicate pagination is complete after the first page.

        Args:
            response: API response object.
        """
        return


class BaseHATEOASPaginator(
    BaseAPIPaginator[t.Optional[ParseResult]],
    metaclass=ABCMeta,
):
    """Paginator class for APIs supporting HATEOAS links in their response bodies.

    HATEOAS stands for "Hypermedia as the Engine of Application State". See
    https://en.wikipedia.org/wiki/HATEOAS.

    This paginator expects responses to have a key "next" with a value
    like "https://api.com/link/to/next-item".

    The :attr:`~singer_sdk.pagination.BaseAPIPaginator.current_value` attribute of
    this paginator is a :class:`urllib.parse.ParseResult` object. This object
    contains the following attributes:

    - scheme
    - netloc
    - path
    - params
    - query
    - fragment

    That means you can access and parse the query params in your stream like this:

    .. code-block:: python

       class MyHATEOASPaginator(BaseHATEOASPaginator):
           def get_next_url(self, response):
               return response.json().get("next")


       class MyStream(Stream):
           def get_new_paginator(self):
               return MyHATEOASPaginator()

           def get_url_params(self, next_page_token) -> dict:
               if next_page_token:
                   return dict(parse_qsl(next_page_token.query))
               return {}
    """

    def __init__(self, *args: t.Any, **kwargs: t.Any) -> None:
        """Create a new paginator.

        Args:
            args: Paginator positional arguments for base class.
            kwargs: Paginator keyword arguments for base class.
        """
        super().__init__(None, *args, **kwargs)

    @abstractmethod
    def get_next_url(self, response: requests.Response) -> str | None:
        """Override this method to extract a HATEOAS link from the response.

        Args:
            response: API response object.
        """
        ...

    @override
    def get_next(self, response: requests.Response) -> ParseResult | None:
        """Get the next pagination token or index from the API response.

        Args:
            response: API response object.

        Returns:
            A parsed HATEOAS link if the response has one, otherwise `None`.
        """
        next_url = self.get_next_url(response)
        return urlparse(next_url) if next_url else None


class HeaderLinkPaginator(BaseHATEOASPaginator):
    """Paginator class for APIs supporting HATEOAS links in their headers.

    Links:
        - https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Link
        - https://datatracker.ietf.org/doc/html/rfc8288#section-3
    """

    @override
    def get_next_url(self, response: requests.Response) -> str | None:
        """Override this method to extract a HATEOAS link from the response.

        Args:
            response: API response object.

        Returns:
            A HATEOAS link parsed from the response headers.
        """
        url: str | None = response.links.get("next", {}).get("url")
        return url


class JSONPathPaginator(BaseAPIPaginator[t.Optional[str]]):
    """Paginator class for APIs returning a pagination token in the response body."""

    def __init__(
        self,
        jsonpath: str,
        *args: t.Any,
        **kwargs: t.Any,
    ) -> None:
        """Create a new paginator.

        Args:
            jsonpath: A JSONPath expression.
            args: Paginator positional arguments for base class.
            kwargs: Paginator keyword arguments for base class.
        """
        super().__init__(None, *args, **kwargs)
        self._jsonpath = jsonpath

    @override
    def get_next(self, response: requests.Response) -> str | None:
        """Get the next page token.

        Args:
            response: API response object.

        Returns:
            The next page token.
        """
        all_matches = extract_jsonpath(self._jsonpath, response.json())
        return next(all_matches, None)


class SimpleHeaderPaginator(BaseAPIPaginator[t.Optional[str]]):
    """Paginator class for APIs returning a pagination token in the response headers."""

    def __init__(
        self,
        key: str,
        *args: t.Any,
        **kwargs: t.Any,
    ) -> None:
        """Create a new paginator.

        Args:
            key: Header key that contains the next page token.
            args: Paginator positional arguments for base class.
            kwargs: Paginator keyword arguments for base class.
        """
        super().__init__(None, *args, **kwargs)
        self._key = key

    @override
    def get_next(self, response: requests.Response) -> str | None:
        """Get the next page token.

        Args:
            response: API response object.

        Returns:
            The next page token.
        """
        return response.headers.get(self._key, None)


class BasePageNumberPaginator(BaseAPIPaginator[int], metaclass=ABCMeta):
    """Paginator class for APIs that use page number."""

    @override
    def get_next(self, response: requests.Response) -> int | None:
        """Get the next page number.

        Args:
            response: API response object.

        Returns:
            The next page number.
        """
        return self._value + 1


class BaseOffsetPaginator(BaseAPIPaginator[int], metaclass=ABCMeta):
    """Paginator class for APIs that use page offset."""

    def __init__(
        self,
        start_value: int,
        page_size: int,
        *args: t.Any,
        **kwargs: t.Any,
    ) -> None:
        """Create a new paginator.

        Args:
            start_value: Initial value.
            page_size: Constant page size.
            args: Paginator positional arguments.
            kwargs: Paginator keyword arguments.
        """
        super().__init__(start_value, *args, **kwargs)
        self._page_size = page_size

    @override
    def get_next(self, response: requests.Response) -> int | None:
        """Get the next page offset.

        Args:
            response: API response object.

        Returns:
            The next page offset.
        """
        return self._value + self._page_size


class LegacyPaginatedStreamProtocol(t.Protocol[TPageToken]):
    """Protocol for legacy paginated streams classes."""

    def get_next_page_token(
        self,
        response: requests.Response,
        previous_token: TPageToken | None,
    ) -> TPageToken | None:
        """Get the next page token.

        Args:
            response: API response object.
            previous_token: Previous page token.
        """


class LegacyStreamPaginator(
    BaseAPIPaginator[t.Optional[TPageToken]],
    t.Generic[TPageToken],
):
    """Paginator that works with REST streams as they exist today."""

    def __init__(
        self,
        stream: LegacyPaginatedStreamProtocol[TPageToken],
        *args: t.Any,
        **kwargs: t.Any,
    ) -> None:
        """Create a new paginator.

        Args:
            stream: A RESTStream instance.
            args: Paginator positional arguments for base class.
            kwargs: Paginator keyword arguments for base class.
        """
        super().__init__(None, *args, **kwargs)
        self.stream = stream

    @override
    def get_next(self, response: requests.Response) -> TPageToken | None:
        """Get next page value by calling the stream method.

        Args:
            response: API response object.

        Returns:
            The next page token or index. Return `None` from this method to indicate
                the end of pagination.
        """
        return self.stream.get_next_page_token(response, self.current_value)
