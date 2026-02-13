from __future__ import annotations

from urllib.parse import urlencode

from singer_sdk.streams.rest import HTTPRequest


def test_custom_encode_params():
    class CustomHTTPRequest(HTTPRequest):
        def encode_params(self) -> str:
            return urlencode(
                self._get_url_params(),
                safe="()",
                doseq=True,
            )

    request = CustomHTTPRequest(
        method="GET",
        url="https://example.com/api",
        headers={"Authorization": "Bearer token"},
        params={"key": "(abc)"},
    )

    assert request.encode_params() == "key=(abc)"
