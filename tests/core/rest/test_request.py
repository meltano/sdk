from __future__ import annotations

import pytest

from singer_sdk.streams.rest import HTTPRequest


def test_set_params():
    request = HTTPRequest(url="https://example.com/api")
    request.params["foo"] = "bar"
    assert request.params == {"foo": "bar"}


def test_set_params_on_string_error():
    request = HTTPRequest(url="https://example.com/api", params="foo=bar")  # ty: ignore[invalid-argument-type]
    with pytest.raises(
        TypeError,
        match="'str' object does not support item assignment",
    ):
        request.params["foo"] = "bar"


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
        params="thisismyquerystring",  # ty: ignore[invalid-argument-type]
    )

    with pytest.deprecated_call():
        assert request.encode_params() == "thisismyquerystring"
