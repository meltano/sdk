"""REST client handling, including DummyJSONStream base class."""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING

from requests_cache import CachedSession
from singer_sdk.pagination import BaseOffsetPaginator
from singer_sdk.streams import RESTStream

from .auth import DummyJSONAuthenticator

if TYPE_CHECKING:
    from singer_sdk.streams.rest import HTTPRequest, HTTPRequestContext

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

PAGE_SIZE = 25


class DummyJSONStream(RESTStream):
    """DummyJSON stream class."""

    records_jsonpath: str = "$[*]"
    timeout = 1

    @property
    @override
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return self.config["api_url"]  # type: ignore[no-any-return]

    @property
    @override
    def requests_session(self) -> CachedSession:
        return CachedSession(
            ".http_cache",
            backend="filesystem",
            serializer="json",
            ignored_parameters=["Authorization", "User-Agent"],
            match_headers=True,
        )

    @property
    @override
    def authenticator(self) -> DummyJSONAuthenticator:
        return DummyJSONAuthenticator(
            base_url=self.config["api_url"],
            username=self.config["username"],
            password=self.config["password"],
        )

    @override
    def get_new_paginator(self) -> BaseOffsetPaginator:
        return BaseOffsetPaginator(start_value=0, page_size=PAGE_SIZE)

    @override
    def get_http_request(self, *, context: HTTPRequestContext[int]) -> HTTPRequest:
        request = super().get_http_request(context=context)
        request.params = {"skip": context.next_page, "limit": PAGE_SIZE}
        return request
