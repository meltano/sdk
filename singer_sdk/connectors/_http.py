"""HTTP-based tap class for Singer SDK."""

from __future__ import annotations

import typing as t

import requests

from singer_sdk.authenticators import NoopAuth
from singer_sdk.connectors.base import BaseConnector

if t.TYPE_CHECKING:
    import sys

    from requests.adapters import BaseAdapter

    if sys.version_info >= (3, 10):
        from typing import TypeAlias  # noqa: ICN003
    else:
        from typing_extensions import TypeAlias

_Auth: TypeAlias = t.Callable[[requests.PreparedRequest], requests.PreparedRequest]


class HTTPConnector(BaseConnector[requests.Session]):
    """Base class for all HTTP-based connectors."""

    def __init__(self, config: t.Mapping[str, t.Any] | None) -> None:
        """Initialize the HTTP connector.

        Args:
            config: Connector configuration parameters.
        """
        super().__init__(config)
        self._session = self.get_session()
        self.refresh_auth()

    def get_connection(self, *, authenticate: bool = True) -> requests.Session:
        """Return a new HTTP session object.

        Adds adapters and optionally authenticates the session.

        Args:
            authenticate: Whether to authenticate the request.

        Returns:
            A new HTTP session object.
        """
        for prefix, adapter in self.adapters.items():
            self._session.mount(prefix, adapter)

        self._session.auth = self._auth if authenticate else None

        return self._session

    def get_session(self) -> requests.Session:
        """Return a new HTTP session object.

        Returns:
            A new HTTP session object.
        """
        return requests.Session()

    def get_authenticator(self) -> _Auth:
        """Authenticate the HTTP session.

        Returns:
            An auth callable.
        """
        return NoopAuth()

    def refresh_auth(self) -> None:
        """Refresh the HTTP session authentication."""
        self._auth = self.get_authenticator()

    @property
    def adapters(self) -> dict[str, BaseAdapter]:
        """Return a mapping of URL prefixes to adapter objects.

        Returns:
            A mapping of URL prefixes to adapter objects.
        """
        return {}

    @property
    def default_request_kwargs(self) -> dict[str, t.Any]:
        """Return default kwargs for HTTP requests.

        Returns:
            A mapping of default kwargs for HTTP requests.
        """
        return {}

    def request(
        self,
        *args: t.Any,
        authenticate: bool = True,
        **kwargs: t.Any,
    ) -> requests.Response:
        """Make an HTTP request.

        Args:
            *args: Positional arguments to pass to the request method.
            authenticate: Whether to authenticate the request.
            **kwargs: Keyword arguments to pass to the request method.

        Returns:
            The HTTP response object.
        """
        with self._connect(authenticate=authenticate) as session:
            kwargs = {**self.default_request_kwargs, **kwargs}
            return session.request(*args, **kwargs)
