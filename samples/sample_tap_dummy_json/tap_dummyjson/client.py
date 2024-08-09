"""REST client handling, including DummyJSONStream base class."""

from __future__ import annotations

from singer_sdk.pagination import BaseOffsetPaginator
from singer_sdk.streams import RESTStream

from .auth import DummyJSONAuthenticator

PAGE_SIZE = 25


class DummyJSONStream(RESTStream):
    """DummyJSON stream class."""

    records_jsonpath: str = ...

    @property
    def url_base(self):
        return self.config["api_url"]

    @property
    def authenticator(self):
        return DummyJSONAuthenticator(
            auth_url=f"{self.url_base}/auth/login",
            refresh_token_url=f"{self.url_base}/refresh",
            username=self.config["username"],
            password=self.config["password"],
        )

    def get_new_paginator(self):
        return BaseOffsetPaginator(start_value=0, page_size=PAGE_SIZE)

    def get_url_params(self, context, next_page_token):
        return {
            "skip": next_page_token,
            "limit": PAGE_SIZE,
        }

    def post_process(self, row, context=None):
        return row
