from __future__ import annotations

import typing as t

from singer_sdk.streams.rest import RESTStream

if t.TYPE_CHECKING:
    from singer_sdk.tap_base import Tap


def test_prepare_request(tap: Tap):
    class MyStream(RESTStream):
        name = "my_stream"
        path = "/my_stream"
        url_base = "https://example.com"

        schema: t.ClassVar[dict[str, t.Any]] = {
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "name": {"type": "string"},
            },
        }

        def get_url_params(
            self,
            context: dict | None,  # noqa: ARG002
            next_page_token: t.Any | None,  # noqa: ARG002
        ) -> dict[str, t.Any] | str:
            return {"foo": "bar"}

        def get_url(
            self,
            context: dict | None,
            *,
            next_page_token: t.Any | None = None,
        ) -> str:
            url = super().get_url(context, next_page_token=next_page_token)
            return f"{url}/page/{next_page_token}"

    stream = MyStream(tap)
    request = stream.prepare_request(context={"foo": "bar"}, next_page_token="baz")  # noqa: S106

    assert request.url == "https://example.com/my_stream/page/baz?foo=bar"
