from __future__ import annotations

from singer_sdk.streams.rest import HTTPRequest


def test_custom_safe_chars():
    request = HTTPRequest(
        method="GET",
        url="https://example.com/api",
        headers={"Authorization": "Bearer token"},
        params={"key": "(abc)"},
        safe_query_chars="()",
    )

    assert request.encode_params() == "key=(abc)"
