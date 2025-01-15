"""Tests generic paginator classes."""

from __future__ import annotations

import json
import typing as t
from urllib.parse import parse_qs, urlparse

import pytest
from requests import PreparedRequest, Response

from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import (
    BaseAPIPaginator,
    BaseHATEOASPaginator,
    BaseOffsetPaginator,
    BasePageNumberPaginator,
    HeaderLinkPaginator,
    JSONPathPaginator,
    SimpleHeaderPaginator,
    SinglePagePaginator,
    first,
)
from singer_sdk.streams.rest import RESTStream

if t.TYPE_CHECKING:
    from singer_sdk.tap_base import Tap


def test_paginator_base_missing_implementation():
    """Validate that `BaseAPIPaginator` implementation requires `get_next`."""

    with pytest.raises(
        TypeError,
        match=r"Can't instantiate abstract class .* '?get_next'?",
    ):
        BaseAPIPaginator(0)


def test_single_page_paginator():
    """Validate single page paginator."""

    response = Response()
    paginator = SinglePagePaginator()
    assert not paginator.finished
    assert paginator.current_value is None
    assert paginator.count == 0

    paginator.advance(response)
    assert paginator.finished
    assert paginator.current_value is None
    assert paginator.count == 1


def test_paginator_hateoas_missing_implementation():
    """Validate that `BaseHATEOASPaginator` implementation requires `get_next_url`."""

    with pytest.raises(
        TypeError,
        match=r"Can't instantiate abstract class .* '?get_next_url'?",
    ):
        BaseHATEOASPaginator()


def test_paginator_attributes():
    """Validate paginator that uses the page number."""

    response = Response()
    paginator = JSONPathPaginator(jsonpath="$.nextPageToken")
    assert str(paginator) == "JSONPathPaginator<None>"

    response._content = b'{"nextPageToken": "abc"}'
    paginator.advance(response)
    assert str(paginator) == "JSONPathPaginator<abc>"


def test_paginator_loop():
    """Validate paginator that uses the page number."""

    response = Response()
    paginator = JSONPathPaginator(jsonpath="$.nextPageToken")
    assert not paginator.finished
    assert paginator.current_value is None
    assert paginator.count == 0

    response._content = b'{"nextPageToken": "abc"}'
    paginator.advance(response)
    assert not paginator.finished
    assert paginator.current_value == "abc"
    assert paginator.count == 1

    response._content = b'{"nextPageToken": "abc"}'
    with pytest.raises(RuntimeError, match="Loop detected in pagination"):
        paginator.advance(response)


def test_paginator_page_number():
    """Validate paginator that uses the page number."""

    class _TestPageNumberPaginator(BasePageNumberPaginator):
        def has_more(self, response: Response) -> bool:
            return response.json()["hasMore"]

    has_more_response = b'{"hasMore": true}'
    no_more_response = b'{"hasMore": false}'

    response = Response()
    paginator = _TestPageNumberPaginator(0)
    assert not paginator.finished
    assert paginator.current_value == 0
    assert paginator.count == 0

    response._content = has_more_response
    paginator.advance(response)
    assert not paginator.finished
    assert paginator.current_value == 1
    assert paginator.count == 1

    response._content = has_more_response
    paginator.advance(response)
    assert not paginator.finished
    assert paginator.current_value == 2
    assert paginator.count == 2

    response._content = no_more_response
    paginator.advance(response)
    assert paginator.finished
    assert paginator.count == 3


def test_paginator_offset():
    """Validate paginator that uses the page offset."""

    class _TestOffsetPaginator(BaseOffsetPaginator):
        def __init__(
            self,
            start_value: int,
            page_size: int,
            records_jsonpath: str,
            *args: t.Any,
            **kwargs: t.Any,
        ) -> None:
            super().__init__(start_value, page_size, *args, **kwargs)
            self._records_jsonpath = records_jsonpath

        def has_more(self, response: Response) -> bool:
            """Check if response has any records.

            Args:
                response: API response object.

            Returns:
                Boolean flag used to indicate if the endpoint has more pages.
            """
            try:
                first(
                    extract_jsonpath(
                        self._records_jsonpath,
                        response.json(),
                    ),
                )
            except StopIteration:
                return False

            return True

    response = Response()
    paginator = _TestOffsetPaginator(0, 2, "$[*]")
    assert not paginator.finished
    assert paginator.current_value == 0
    assert paginator.count == 0

    response._content = b'[{"id": 1}, {"id": 2}]'
    paginator.advance(response)
    assert not paginator.finished
    assert paginator.current_value == 2
    assert paginator.count == 1

    response._content = b'[{"id": 3}]'
    paginator.advance(response)
    assert not paginator.finished
    assert paginator.current_value == 4
    assert paginator.count == 2

    response._content = b"[]"
    paginator.advance(response)
    assert paginator.finished
    assert paginator.count == 3


def test_paginator_jsonpath():
    """Validate paginator that uses JSONPath."""

    response = Response()
    paginator = JSONPathPaginator(jsonpath="$.nextPageToken")
    assert not paginator.finished
    assert paginator.current_value is None
    assert paginator.count == 0

    response._content = b'{"nextPageToken": "abc"}'
    paginator.advance(response)
    assert not paginator.finished
    assert paginator.current_value == "abc"
    assert paginator.count == 1

    response._content = b'{"nextPageToken": null}'
    paginator.advance(response)
    assert paginator.finished
    assert paginator.count == 2


def test_paginator_header():
    """Validate paginator that uses response headers."""

    key = "X-Next-Page"
    response = Response()
    paginator = SimpleHeaderPaginator(key=key)
    assert not paginator.finished
    assert paginator.current_value is None
    assert paginator.count == 0

    response.headers[key] = "abc"
    paginator.advance(response)
    assert not paginator.finished
    assert paginator.current_value == "abc"
    assert paginator.count == 1

    response.headers[key] = None
    paginator.advance(response)
    assert paginator.finished
    assert paginator.count == 2


def test_paginator_header_links():
    """Validate paginator that uses HATEOAS links."""

    api_hostname = "my.api.test"
    resource_path = "/path/to/resource"

    response = Response()
    paginator = HeaderLinkPaginator()
    assert not paginator.finished
    assert paginator.current_value is None
    assert paginator.count == 0

    response.headers.update(
        {"Link": f"<https://{api_hostname}{resource_path}?page=2&limit=100>; rel=next"},
    )
    paginator.advance(response)
    assert not paginator.finished
    assert paginator.current_value.hostname == api_hostname
    assert paginator.current_value.path == resource_path
    assert paginator.current_value.query == "page=2&limit=100"
    assert paginator.count == 1

    response.headers.update(
        {
            "Link": (
                f"<https://{api_hostname}{resource_path}?page=3&limit=100>;rel=next,"
                f"<https://{api_hostname}{resource_path}?page=2&limit=100>;rel=back"
            ),
        },
    )
    paginator.advance(response)
    assert not paginator.finished
    assert paginator.current_value.hostname == api_hostname
    assert paginator.current_value.path == resource_path
    assert paginator.current_value.query == "page=3&limit=100"
    assert paginator.count == 2

    response.headers.update(
        {"Link": "<https://myapi.test/path/to/resource?page=3&limit=100>;rel=back"},
    )
    paginator.advance(response)
    assert paginator.finished
    assert paginator.count == 3


def test_paginator_custom_hateoas():
    """Validate paginator that uses HATEOAS links."""

    class _CustomHATEOASPaginator(BaseHATEOASPaginator):
        def get_next_url(self, response: Response) -> str | None:
            """Get a parsed HATEOAS link for the next, if the response has one."""

            try:
                return first(
                    extract_jsonpath(
                        "$.links[?(@.rel=='next')].href",
                        response.json(),
                    ),
                )
            except StopIteration:
                return None

    resource_path = "/path/to/resource"

    response = Response()
    paginator = _CustomHATEOASPaginator()
    assert not paginator.finished
    assert paginator.current_value is None
    assert paginator.count == 0

    response._content = json.dumps(
        {
            "links": [
                {
                    "rel": "next",
                    "href": f"{resource_path}?page=2&limit=100",
                },
            ],
        },
    ).encode()
    paginator.advance(response)
    assert not paginator.finished
    assert paginator.current_value.path == resource_path
    assert paginator.current_value.query == "page=2&limit=100"
    assert paginator.count == 1

    response._content = json.dumps(
        {
            "links": [
                {
                    "rel": "next",
                    "href": f"{resource_path}?page=3&limit=100",
                },
            ],
        },
    ).encode()
    paginator.advance(response)
    assert not paginator.finished
    assert paginator.current_value.path == resource_path
    assert paginator.current_value.query == "page=3&limit=100"
    assert paginator.count == 2

    response._content = json.dumps({"links": []}).encode()
    paginator.advance(response)
    assert paginator.finished
    assert paginator.count == 3


def test_break_pagination(tap: Tap, caplog: pytest.LogCaptureFixture):
    class MyAPIStream(RESTStream[int]):
        """My API stream."""

        name = "my-api-stream"
        url_base = "https://my.api.test"
        path = "/path/to/resource"
        schema = {"type": "object", "properties": {"id": {"type": "integer"}}}  # noqa: RUF012

        def parse_response(self, response: Response) -> t.Iterable[dict]:
            return response.json()

        def get_new_paginator(self) -> BasePageNumberPaginator:
            return BasePageNumberPaginator(1)

        def get_url_params(
            self,
            context: dict | None,  # noqa: ARG002
            next_page_token: int | None,
        ) -> dict[str, t.Any] | str:
            params = {}
            if next_page_token:
                params["page"] = next_page_token
            return params

        def _request(
            self,
            prepared_request: PreparedRequest,
            context: dict | None,  # noqa: ARG002
        ) -> Response:
            r = Response()
            r.status_code = 200

            parsed = urlparse(prepared_request.url)
            query = parse_qs(parsed.query)

            if query.get("page", ["1"]) == ["1"]:
                r._content = json.dumps(
                    [
                        {"id": 1},
                        {"id": 2},
                    ]
                ).encode()
            else:
                r._content = json.dumps([]).encode()

            return r

    stream = MyAPIStream(tap=tap)

    records_iter = stream.request_records(context=None)

    next(records_iter)
    next(records_iter)
    with pytest.raises(StopIteration):
        next(records_iter)

    assert "Pagination stopped after 1 pages" in caplog.text
