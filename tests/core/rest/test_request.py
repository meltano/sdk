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


def test_encode_non_string_params():
    request = HTTPRequest(
        method="GET",
        url="https://example.com/api",
        headers={"Authorization": "Bearer token"},
        params={
            "number": 123,
            "bool": True,
            "list": [1, 2, 3],
            "tuple": ("a", "b", "c"),
            "null": None,
        },
        safe_query_chars="",
    )

    assert (
        request.encode_params()
        == "number=123&bool=True&list=1&list=2&list=3&tuple=a&tuple=b&tuple=c"
    )


def test_string_params_passthrough():
    request = HTTPRequest(
        method="GET",
        url="https://example.com/api",
        headers={"Authorization": "Bearer token"},
        params="thisismyquerystring",
    )

    assert request.encode_params() == "thisismyquerystring"
