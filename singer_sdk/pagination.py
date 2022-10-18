"""Generic paginator classes."""

from __future__ import annotations

import sys
from abc import ABCMeta, abstractmethod
from typing import Any, Generic, Iterable, Optional, TypeVar
from urllib.parse import ParseResult, urlparse

from requests import Response

from singer_sdk.helpers.jsonpath import extract_jsonpath

if sys.version_info >= (3, 8):
    from typing import Protocol
else:
    from typing_extensions import Protocol

T = TypeVar("T")
TPageToken = TypeVar("TPageToken")


def first(iterable: Iterable[T]) -> T:
    """Return the first element of an iterable or raise an exception.

    Args:
        iterable: An iterable.

    Returns:
        The first element of the iterable.

    >>> first('ABC')
    'A'
    """
    return next(iter(iterable))


class BaseAPIPaginator(Generic[TPageToken], metaclass=ABCMeta):
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

    def advance(self, response: Response) -> None:
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
            raise RuntimeError(
                f"Loop detected in pagination. "
                f"Pagination token {new_value} is identical to prior token."
            )

        # Stop if new value None, empty string, 0, etc.
        if not new_value:
            self._finished = True
        else:
            self._value = new_value

    def has_more(self, response: Response) -> bool:
        """Override this method to check if the endpoint has any pages left.

        Args:
            response: API response object.

        Returns:
            Boolean flag used to indicate if the endpoint has more pages.
        """
        return True

    @abstractmethod
    def get_next(self, response: Response) -> TPageToken | None:
        """Get the next pagination token or index from the API response.

        Args:
            response: API response object.

        Returns:
            The next page token or index. Return `None` from this method to indicate
                the end of pagination.
        """
        ...


class SinglePagePaginator(BaseAPIPaginator[None]):
    """A paginator that does works with single-page endpoints."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Create a new paginator.

        Args:
            args: Paginator positional arguments for base class.
            kwargs: Paginator keyword arguments for base class.
        """
        super().__init__(None, *args, **kwargs)

    def get_next(self, response: Response) -> None:
        """Get the next pagination token or index from the API response.

        Args:
            response: API response object.

        Returns:
            The next page token or index. Return `None` from this method to indicate
                the end of pagination.
        """
        return None


class BaseHATEOASPaginator(BaseAPIPaginator[Optional[ParseResult]], metaclass=ABCMeta):
    """Paginator class for APIs supporting HATEOAS links in their response bodies.

    HATEOAS stands for "Hypermedia as the Engine of Application State". See
    https://en.wikipedia.org/wiki/HATEOAS.

    This paginator expects responses to have a key "next" with a value
    like "https://api.com/link/to/next-item".

    The :attr:`~singer_sdk.pagination.BaseAPIPaginator.current_value` attribute of
    this paginator is a `urllib.parse.ParseResult`_ object. This object
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

    .. _`urllib.parse.ParseResult`:
         https://docs.python.org/3/library/urllib.parse.html#urllib.parse.urlparse
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Create a new paginator.

        Args:
            args: Paginator positional arguments for base class.
            kwargs: Paginator keyword arguments for base class.
        """
        super().__init__(None, *args, **kwargs)

    @abstractmethod
    def get_next_url(self, response: Response) -> str | None:
        """Override this method to extract a HATEOAS link from the response.

        Args:
            response: API response object.
        """
        ...

    def get_next(self, response: Response) -> ParseResult | None:
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

    def get_next_url(self, response: Response) -> str | None:
        """Override this method to extract a HATEOAS link from the response.

        Args:
            response: API response object.

        Returns:
            A HATEOAS link parsed from the response headers.
        """
        url: str | None = response.links.get("next", {}).get("url")
        return url


class JSONPathPaginator(BaseAPIPaginator[Optional[str]]):
    """Paginator class for APIs returning a pagination token in the response body."""

    def __init__(
        self,
        jsonpath: str,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """Create a new paginator.

        Args:
            jsonpath: A JSONPath expression.
            args: Paginator positional arguments for base class.
            kwargs: Paginator keyword arguments for base class.
        """
        super().__init__(None, *args, **kwargs)
        self._jsonpath = jsonpath

    def get_next(self, response: Response) -> str | None:
        """Get the next page token.

        Args:
            response: API response object.

        Returns:
            The next page token.
        """
        all_matches = extract_jsonpath(self._jsonpath, response.json())
        return next(all_matches, None)


class SimpleHeaderPaginator(BaseAPIPaginator[Optional[str]]):
    """Paginator class for APIs returning a pagination token in the response headers."""

    def __init__(
        self,
        key: str,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """Create a new paginator.

        Args:
            key: Header key that contains the next page token.
            args: Paginator positional arguments for base class.
            kwargs: Paginator keyword arguments for base class.
        """
        super().__init__(None, *args, **kwargs)
        self._key = key

    def get_next(self, response: Response) -> str | None:
        """Get the next page token.

        Args:
            response: API response object.

        Returns:
            The next page token.
        """
        return response.headers.get(self._key, None)


class BasePageNumberPaginator(BaseAPIPaginator[int], metaclass=ABCMeta):
    """Paginator class for APIs that use page number."""

    @abstractmethod
    def has_more(self, response: Response) -> bool:
        """Override this method to check if the endpoint has any pages left.

        Args:
            response: API response object.

        Returns:
            Boolean flag used to indicate if the endpoint has more pages.

        """
        ...

    def get_next(self, response: Response) -> int | None:
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
        *args: Any,
        **kwargs: Any,
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

    @abstractmethod
    def has_more(self, response: Response) -> bool:
        """Override this method to check if the endpoint has any pages left.

        Args:
            response: API response object.

        Returns:
            Boolean flag used to indicate if the endpoint has more pages.
        """
        ...

    def get_next(self, response: Response) -> int | None:
        """Get the next page offset.

        Args:
            response: API response object.

        Returns:
            The next page offset.
        """
        return self._value + self._page_size


class LegacyPaginatedStreamProtocol(Protocol[TPageToken]):
    """Protocol for legacy paginated streams classes."""

    def get_next_page_token(
        self,
        response: Response,
        previous_token: TPageToken | None,
    ) -> TPageToken | None:
        """Get the next page token.

        Args:
            response: API response object.
            previous_token: Previous page token.
        """
        ...  # pragma: no cover


class LegacyStreamPaginator(
    BaseAPIPaginator[Optional[TPageToken]],
    Generic[TPageToken],
):
    """Paginator that works with REST streams as they exist today."""

    def __init__(
        self,
        stream: LegacyPaginatedStreamProtocol[TPageToken],
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """Create a new paginator.

        Args:
            stream: A RESTStream instance.
            args: Paginator positional arguments for base class.
            kwargs: Paginator keyword arguments for base class.
        """
        super().__init__(None, *args, **kwargs)
        self.stream = stream

    def get_next(self, response: Response) -> TPageToken | None:
        """Get next page value by calling the stream method.

        Args:
            response: API response object.

        Returns:
            The next page token or index. Return `None` from this method to indicate
                the end of pagination.
        """
        return self.stream.get_next_page_token(response, self.current_value)
