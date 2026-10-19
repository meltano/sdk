from __future__ import annotations

import pytest

from singer_sdk.helpers import jsonpath
from singer_sdk.helpers.jsonpath import extract_jsonpath


def test_root_wildcard_single_object():
    """A lone object under ``$[*]`` is a single record (jsonpath-ng>=1.11 compat)."""
    assert list(extract_jsonpath("$[*]", {"a": 1})) == [{"a": 1}]
    assert list(extract_jsonpath("$[*]", [{"a": 1}, {"a": 2}])) == [
        {"a": 1},
        {"a": 2},
    ]


@pytest.mark.filterwarnings("default::PendingDeprecationWarning")
def test_nested_wildcard_single_object():
    data = {"data": {"a": 1}}
    with pytest.warns(PendingDeprecationWarning, match="non-array"):
        assert list(extract_jsonpath("$.data[*]", data)) == [{"a": 1}]


def test_nested_wildcard_list():
    data = {"data": [{"a": 1}, {"a": 2}], "missing": None}
    assert list(extract_jsonpath("$.data[*]", data)) == [{"a": 1}, {"a": 2}]
    assert list(extract_jsonpath("$.missing[*]", data)) == []
    assert list(extract_jsonpath("$.nope[*]", data)) == []


def test_non_wildcard_expressions_unchanged():
    data = {"data": [{"a": 1}, {"a": 2}]}
    assert list(extract_jsonpath("$.data[0:1]", data)) == [{"a": 1}]
    assert list(extract_jsonpath("$.data[?(@.a==2)]", data)) == [{"a": 2}]


@pytest.mark.filterwarnings("default::PendingDeprecationWarning")
def test_descendant_wildcard():
    data = {"items": [{"a": 1}], "x": {"items": {"b": 2}}}
    with pytest.warns(PendingDeprecationWarning, match="non-array"):
        assert list(extract_jsonpath("$..items[*]", data)) == [{"a": 1}, {"b": 2}]


def test_bare_descendant_wildcard_not_rewritten():
    """``$..[*]`` has no parent path to evaluate, so it is left to jsonpath-ng."""
    assert not jsonpath._is_wildcard_child(jsonpath._compile_jsonpath("$..[*]"))
    assert jsonpath._is_wildcard_child(jsonpath._compile_jsonpath("$..items[*]"))


@pytest.mark.parametrize(
    "version,ref",
    [
        pytest.param("0.54.8", "v0.54.8", id="release"),
        pytest.param("0.55.0b4.dev20+g8363ee56c", "latest", id="dev"),
        pytest.param("[could not be detected]", "latest", id="unknown"),
    ],
)
def test_docs_url(monkeypatch: pytest.MonkeyPatch, version: str, ref: str):
    monkeypatch.setattr(jsonpath, "get_sdk_version", lambda: version)
    assert jsonpath._docs_url() == f"https://sdk.meltano.com/en/{ref}/code_samples.html"
