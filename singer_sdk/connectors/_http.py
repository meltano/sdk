"""HTTP-based tap class for Singer SDK."""

from __future__ import annotations

import typing as t

import requests

from singer_sdk.authenticators import NoopAuth
from singer_sdk.connectors.base import BaseConnector

if t.TYPE_CHECKING:
    import sys
    from collections.abc import Mapping

    from requests.adapters import BaseAdapter

    if sys.version_info >= (3, 10):
        from typing import TypeAlias  # noqa: ICN003
    else:
        from typing_extensions import TypeAlias

_Auth: TypeAlias = t.Callable[[requests.PreparedRequest], requests.PreparedRequest]


class HTTPConnector(BaseConnector[requests.Session]):
    """Base class for all HTTP-based connectors."""

    def __init__(self, config: Mapping[str, t.Any] | None = None) -> None:
        """Initialize the HTTP connector.

        Args:
            config: Connector configuration parameters.
        """
        super().__init__(config)
        self.__session = self.get_session()
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
            self.__session.mount(prefix, adapter)

        self.__session.auth = self.auth if authenticate else None

        return self.__session

    def get_session(self) -> requests.Session:  # noqa: PLR6301
        """Return a new HTTP session object.

        Returns:
            A new HTTP session object.
        """
        return requests.Session()

    def get_authenticator(self) -> _Auth:  # noqa: PLR6301
        """Authenticate the HTTP session.

        Returns:
            An auth callable.
        """
        return NoopAuth()

    def refresh_auth(self) -> None:
        """Refresh the HTTP session authentication."""
        self.auth = self.get_authenticator()

    @property
    def auth(self) -> _Auth:
        """Return the HTTP session authenticator.

        Returns:
            An auth callable.
        """
        return self.__auth

    @auth.setter
    def auth(self, auth: _Auth) -> None:
        """Set the HTTP session authenticator.

        Args:
            auth: An auth callable.
        """
        self.__auth = auth

    @property
    def session(self) -> requests.Session:
        """Return the HTTP session object.

        Returns:
            The HTTP session object.
        """
        return self.__session

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
        with self.connect(authenticate=authenticate) as session:
            kwargs = {**self.default_request_kwargs, **kwargs}
            return session.request(*args, **kwargs)
