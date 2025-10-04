"""REST client handling, including DummyJSONStream base class."""

from __future__ import annotations

import sys

from requests_cache import CachedSession
from singer_sdk.pagination import BaseOffsetPaginator
from singer_sdk.streams import RESTStream

from .auth import DummyJSONAuthenticator

if sys.version_info < (3, 12):
    from typing_extensions import override
else:
    from typing import override

PAGE_SIZE = 25


class DummyJSONStream(RESTStream):
    """DummyJSON stream class."""

    records_jsonpath: str = "$[*]"

    @property
    @override
    def url_base(self):
        return self.config["api_url"]

    @property
    @override
    def requests_session(self) -> CachedSession:
        return CachedSession(
            ".http_cache",
            backend="filesystem",
            serializer="json",
        )

    @property
    @override
    def authenticator(self):
        return DummyJSONAuthenticator(
            auth_url=f"{self.url_base}/auth/login",
            refresh_token_url=f"{self.url_base}/refresh",
            username=self.config["username"],
            password=self.config["password"],
        )

    @override
    def get_new_paginator(self):
        return BaseOffsetPaginator(start_value=0, page_size=PAGE_SIZE)

    @override
    def get_url_params(self, context, next_page_token):
        return {
            "skip": next_page_token,
            "limit": PAGE_SIZE,
        }

    @override
    def post_process(self, row, context=None):
        return row
