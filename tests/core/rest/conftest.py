"""REST fixtures."""

from __future__ import annotations

import typing as t
from functools import cached_property

import pytest
from requests.auth import HTTPProxyAuth

from singer_sdk.authenticators import APIAuthenticatorBase, SingletonMeta
from singer_sdk.streams import RESTStream
from singer_sdk.tap_base import Tap


class SingletonAuthenticator(APIAuthenticatorBase, metaclass=SingletonMeta):
    """A singleton authenticator."""


class SimpleRESTStream(RESTStream):
    """A REST stream for testing."""

    url_base = "https://example.com"
    schema: t.ClassVar[dict] = {
        "type": "object",
        "properties": {},
    }
    path = "/dummy"

    @property
    def authenticator(self) -> APIAuthenticatorBase:
        """Stream authenticator."""
        return APIAuthenticatorBase(stream=self)


class SingletonAuthStream(SimpleRESTStream):
    """A stream with singleton authenticator."""

    @property
    def authenticator(self) -> SingletonAuthenticator:
        """Stream authenticator."""
        return SingletonAuthenticator(stream=self)


class NaiveAuthenticator(APIAuthenticatorBase):
    """A naive authenticator class."""


class CachedAuthStream(SimpleRESTStream):
    """A stream with Naive authentication."""

    @cached_property
    def authenticator(self) -> NaiveAuthenticator:
        """Stream authenticator."""
        return NaiveAuthenticator(stream=self)


class ProxyAuthStream(SimpleRESTStream):
    """A stream with digest authentication."""

    @property
    def authenticator(self):
        """Stream authenticator."""
        return HTTPProxyAuth("username", "password")


class SimpleTap(Tap):
    """A REST tap for testing."""

    name = "tappy"

    def discover_streams(self):
        """Get collection of streams."""
        return [
            SimpleRESTStream(self, name="some_stream"),
            SimpleRESTStream(self, name="other_stream"),
            SingletonAuthStream(self, name="single_auth_stream"),
            SingletonAuthStream(self, name="reused_single_auth_stream"),
            CachedAuthStream(self, name="cached_auth_stream"),
            CachedAuthStream(self, name="other_cached_auth_stream"),
            ProxyAuthStream(self, name="proxy_auth_stream"),
        ]


@pytest.fixture
def rest_tap():
    """Create a RESTful tap instance."""
    return SimpleTap()
