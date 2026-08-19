from __future__ import annotations

import json
from pathlib import Path

import pytest
import requests
from requests_cache import CachedResponse
from requests_cache.cache_keys import redact_response

from singer_sdk.helpers.http_cache import (
    SafeCachedSession,
    _cache_key,
    _decode_text,
    _json_credentials,
    _strip_form_body,
    _strip_json_body,
    _strip_url,
)


class _SyntheticAdapter(requests.adapters.BaseAdapter):
    def __init__(
        self,
        *,
        response_url: str | None = None,
        redirect_to: str | None = None,
        body: str | None = None,
        content_type: str = "application/json",
    ) -> None:
        self.response_url = response_url
        self.redirect_to = redirect_to
        self.body = body
        self.content_type = content_type
        self.seen_headers = {}
        self.seen_body = None
        self.seen_requests: list[requests.PreparedRequest] = []

    def send(self, request, **kwargs):  # noqa: ARG002
        self.seen_requests.append(request)
        self.seen_headers = dict(request.headers)
        self.seen_body = request.body
        response = requests.Response()
        response.url = self.response_url or request.url
        response.request = request
        headers = {
            "Content-Type": self.content_type,
            "Set-Cookie": "session=SYNTHETIC-COOKIE-SECRET",
        }
        if self.redirect_to and len(self.seen_requests) == 1:
            response.status_code = 302
            headers["Location"] = self.redirect_to
        else:
            response.status_code = 200
        response.headers.update(headers)
        response.cookies.set("session", "SYNTHETIC-COOKIE-SECRET")
        response._content = (
            self.body or json.dumps({"call": len(self.seen_requests)})
        ).encode()
        response.raw = requests.packages.urllib3.response.HTTPResponse(
            body=response._content,
            headers=headers,
            status=response.status_code,
            request_url=request.url,
            preload_content=False,
        )
        return response

    def close(self) -> None:
        pass


def _cache_files(cache_path: Path) -> list[Path]:
    return [path for path in cache_path.rglob("*.json") if path.is_file()]


def _persisted(cache_path: Path) -> bytes:
    """Return every byte written to the response cache, asserting some was."""
    files = _cache_files(cache_path)
    assert files, "no cache file was written"
    persisted = b"\n".join(path.read_bytes() for path in files)
    assert persisted.strip(), "cache files are empty"
    return persisted


def test_cache_files_never_persist_synthetic_credentials(tmp_path: Path) -> None:
    marker_values = {
        "SYNTHETIC-AUTH-SECRET",
        "SYNTHETIC-GITLAB-SECRET",
        "SYNTHETIC-COOKIE-SECRET",
        "SYNTHETIC-QUERY-SECRET",
        "SYNTHETIC-BODY-SECRET",
        "SYNTHETIC-USERINFO-SECRET",
    }
    with SafeCachedSession(cache_path=tmp_path, allowable_methods=("POST",)) as session:
        adapter = _SyntheticAdapter(
            response_url=(
                "https://user:SYNTHETIC-USERINFO-SECRET@example.invalid/result"
            )
        )
        session.mount("https://", adapter)
        response = session.post(
            "https://example.invalid/data?access_token=SYNTHETIC-QUERY-SECRET&safe=yes",
            headers={
                "Authorization": "Bearer SYNTHETIC-AUTH-SECRET",
                "Private-Token": "SYNTHETIC-GITLAB-SECRET",
                "Cookie": "session=SYNTHETIC-COOKIE-SECRET",
            },
            json={"token": "SYNTHETIC-BODY-SECRET", "safe": "yes"},
        )

    assert response.status_code == 200
    assert "SYNTHETIC-AUTH-SECRET" in adapter.seen_headers["Authorization"]
    assert b"SYNTHETIC-BODY-SECRET" in adapter.seen_body
    persisted = _persisted(tmp_path).lower()
    for marker in marker_values:
        assert marker.lower().encode() not in persisted
    for header in (b"authorization", b"private-token", b"set-cookie", b"cookie"):
        assert header not in persisted


def test_requests_cache_still_redacts_canonically_cased_headers(
    tmp_path: Path,
) -> None:
    """requests-cache is the second layer, and it matches header names as sent.

    ``filter_sort_dict`` tests ``name in ignored_parameters`` against the original
    casing yielded by ``CaseInsensitiveDict.items()``, so a lower-case-only list
    silently disables its own redaction.
    """
    adapter = _SyntheticAdapter()
    prepared = requests.Request(
        "GET",
        "https://example.invalid/data",
        headers={"Authorization": "Bearer SYNTHETIC-AUTH-SECRET"},
    ).prepare()

    with SafeCachedSession(cache_path=tmp_path) as session:
        redacted = redact_response(
            CachedResponse.from_response(adapter.send(prepared)),
            session.settings.ignored_parameters,
        )

    assert redacted.headers["Set-Cookie"] == "REDACTED"
    assert redacted.request.headers["Authorization"] == "REDACTED"


def test_cache_files_never_persist_credentials_sent_via_send(tmp_path: Path) -> None:
    """A request prepared elsewhere must not skip sanitisation.

    ``requests.Session.send`` only dispatches ``request.hooks``; session hooks are
    merged in by ``prepare_request``, so a hook is no control at all here.
    """
    with SafeCachedSession(cache_path=tmp_path) as session:
        session.mount("https://", _SyntheticAdapter())
        prepared = requests.Request(
            "GET",
            "https://example.invalid/data?access_token=SYNTHETIC-QUERY-SECRET",
            headers={"Authorization": "Bearer SYNTHETIC-AUTH-SECRET"},
        ).prepare()
        response = session.send(prepared)

    assert response.status_code == 200
    persisted = _persisted(tmp_path).lower()
    for marker in (b"synthetic-auth-secret", b"synthetic-query-secret"):
        assert marker not in persisted
    assert b"authorization" not in persisted


def test_cache_files_never_persist_response_body_credentials(tmp_path: Path) -> None:
    body = json.dumps({"access_token": "SYNTHETIC-SESSION-SECRET", "safe": "yes"})
    with SafeCachedSession(cache_path=tmp_path) as session:
        session.mount("https://", _SyntheticAdapter(body=body))
        response = session.get("https://example.invalid/session")

    assert response.text == body
    persisted = _persisted(tmp_path)
    assert b"SYNTHETIC-SESSION-SECRET" not in persisted
    assert b"safe" in persisted


def test_cache_hit_is_served_without_a_second_request(tmp_path: Path) -> None:
    with SafeCachedSession(cache_path=tmp_path) as session:
        adapter = _SyntheticAdapter()
        session.mount("https://", adapter)
        first = session.get("https://example.invalid/data")
        second = session.get("https://example.invalid/data")

    assert len(adapter.seen_requests) == 1
    assert not first.from_cache
    assert second.from_cache
    assert second.json() == first.json()


def test_distinct_credentials_do_not_share_a_cache_entry(tmp_path: Path) -> None:
    """Two tenants must never be served each other's cached body."""
    with SafeCachedSession(cache_path=tmp_path) as session:
        adapter = _SyntheticAdapter()
        session.mount("https://", adapter)
        first = session.get("https://example.invalid/data?access_token=TENANT-A")
        second = session.get("https://example.invalid/data?access_token=TENANT-B")

    assert len(adapter.seen_requests) == 2
    assert not second.from_cache
    assert first.json() == {"call": 1}
    assert second.json() == {"call": 2}


def test_distinct_authorization_headers_do_not_share_a_cache_entry(
    tmp_path: Path,
) -> None:
    with SafeCachedSession(cache_path=tmp_path) as session:
        adapter = _SyntheticAdapter()
        session.mount("https://", adapter)
        first = session.get(
            "https://example.invalid/data",
            headers={"Authorization": "Bearer TENANT-A"},
        )
        second = session.get(
            "https://example.invalid/data",
            headers={"Authorization": "Bearer TENANT-B"},
        )

    assert len(adapter.seen_requests) == 2
    assert not second.from_cache
    assert first.json() == {"call": 1}
    assert second.json() == {"call": 2}


def test_pagination_cursor_named_token_is_not_collapsed(tmp_path: Path) -> None:
    """``token`` is a credential name *and* a common cursor name."""
    with SafeCachedSession(cache_path=tmp_path) as session:
        adapter = _SyntheticAdapter()
        session.mount("https://", adapter)
        page_1 = session.get("https://example.invalid/data?token=page-1")
        page_2 = session.get("https://example.invalid/data?token=page-2")

    assert len(adapter.seen_requests) == 2
    assert page_1.json() == {"call": 1}
    assert page_2.json() == {"call": 2}


def test_redirect_hop_keeps_its_credentials(tmp_path: Path) -> None:
    """Sanitising the live response would strip the request 302 handling reuses."""
    with SafeCachedSession(cache_path=tmp_path) as session:
        adapter = _SyntheticAdapter(redirect_to="https://example.invalid/moved")
        session.mount("https://", adapter)
        response = session.get(
            "https://example.invalid/data",
            headers={"Authorization": "Bearer SYNTHETIC-AUTH-SECRET"},
            allow_redirects=True,
        )

    assert len(adapter.seen_requests) == 2
    assert response.history
    followed = adapter.seen_requests[1]
    assert followed.headers["Authorization"] == "Bearer SYNTHETIC-AUTH-SECRET"
    assert b"SYNTHETIC-AUTH-SECRET" not in _persisted(tmp_path)


def test_next_request_keeps_its_credentials(tmp_path: Path) -> None:
    with SafeCachedSession(cache_path=tmp_path) as session:
        session.mount(
            "https://", _SyntheticAdapter(redirect_to="https://example.invalid/moved")
        )
        response = session.get(
            "https://example.invalid/data",
            headers={"Authorization": "Bearer SYNTHETIC-AUTH-SECRET"},
            allow_redirects=False,
        )

    assert response.status_code == 302
    assert response.next is not None
    assert response.next.headers["Authorization"] == "Bearer SYNTHETIC-AUTH-SECRET"
    # A 302 is not a cacheable status, so nothing should have been written at all.
    assert not _cache_files(tmp_path)


def test_caller_still_sees_response_cookies(tmp_path: Path) -> None:
    with SafeCachedSession(cache_path=tmp_path) as session:
        session.mount("https://", _SyntheticAdapter())
        response = session.get("https://example.invalid/data")

    assert response.cookies["session"] == "SYNTHETIC-COOKIE-SECRET"
    assert response.headers["Set-Cookie"] == "session=SYNTHETIC-COOKIE-SECRET"
    assert b"SYNTHETIC-COOKIE-SECRET" not in _persisted(tmp_path)


def test_cache_clears_unproven_post_bodies(tmp_path: Path) -> None:
    markers_and_content_types = (
        ("SYNTHETIC-MALFORMED-JSON", "application/json"),
        ("SYNTHETIC-MALFORMED-FORM", "application/x-www-form-urlencoded"),
        ("SYNTHETIC-OPAQUE-BODY", "application/octet-stream"),
    )
    with SafeCachedSession(cache_path=tmp_path, allowable_methods=("POST",)) as session:
        session.mount("https://", _SyntheticAdapter())
        for index, (marker, content_type) in enumerate(markers_and_content_types):
            response = session.post(
                f"https://example.invalid/body/{index}",
                headers={"Content-Type": content_type},
                data=marker,
            )
            assert response.status_code == 200

    persisted = _persisted(tmp_path)
    for marker, _ in markers_and_content_types:
        assert marker.encode() not in persisted


def test_default_cache_is_outside_current_directory(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.chdir(tmp_path)

    with SafeCachedSession() as session:
        assert not Path(session.cache.cache_dir).is_relative_to(tmp_path)
    assert not (tmp_path / ".http_cache").exists()


def test_default_cache_location_is_stable_across_sessions() -> None:
    """A per-instance temporary directory would make every request a miss."""
    with (
        SafeCachedSession(cache_name="test-stable") as first,
        SafeCachedSession(cache_name="test-stable") as second,
    ):
        assert first.cache.cache_dir == second.cache.cache_dir

    with SafeCachedSession(cache_name="test-other") as other:
        assert other.cache.cache_dir != first.cache.cache_dir


@pytest.mark.parametrize(
    "value,expected",
    [
        pytest.param("text", "text", id="str"),
        pytest.param(b"text", "text", id="bytes"),
        pytest.param(bytearray(b"text"), "text", id="bytearray"),
        pytest.param(b"\xff\xfe", None, id="undecodable-bytes"),
        pytest.param(None, None, id="none"),
        pytest.param(object(), None, id="unsupported-type"),
    ],
)
def test_decode_text(value: object, expected: str | None) -> None:
    assert _decode_text(value) == expected


def test_empty_url_is_returned_unchanged() -> None:
    assert not _strip_url("")


def test_url_credentials_are_removed() -> None:
    assert (
        _strip_url("https://user:pass@example.invalid/x?token=secret&safe=kept")
        == "https://example.invalid/x?safe=kept"
    )


def test_undecodable_bodies_are_dropped() -> None:
    assert _strip_form_body(None) is None
    assert _strip_json_body(None) is None


def test_form_body_keeps_safe_parameters() -> None:
    assert (
        _strip_form_body("token=SYNTHETIC-FORM-SECRET&safe=form-kept")
        == "safe=form-kept"
    )


def test_json_body_sanitises_nested_lists() -> None:
    stripped = _strip_json_body(
        '{"items": [{"token": "SYNTHETIC-LIST-SECRET"}, {"safe": "list-kept"}]}'
    )
    assert stripped is not None
    assert json.loads(stripped) == {"items": [{}, {"safe": "list-kept"}]}


def test_credentials_are_key_material_at_any_depth() -> None:
    """requests-cache redacts only top-level parameters, so nesting is the gap.

    A session-level test cannot prove this: a body nested one level deep already
    produces two distinct requests-cache keys on its own, so the recursion below
    is what keeps a *redacted* nested credential apart, not what separates the
    bodies.
    """
    nested_in_list = dict(_json_credentials({"items": [{"token": "IN-A-LIST"}]}))
    nested_in_dict = dict(_json_credentials({"outer": {"token": "IN-A-DICT"}}))

    assert nested_in_list == {"token": '"IN-A-LIST"'}
    assert nested_in_dict == {"token": '"IN-A-DICT"'}


def test_url_userinfo_does_not_share_a_cache_entry() -> None:
    """Userinfo is a credential, and requests-cache keys on it without our help."""
    first = requests.PreparedRequest()
    first.prepare(method="GET", url="https://tenant:USERINFO-A@example.invalid/data")
    second = requests.PreparedRequest()
    second.prepare(method="GET", url="https://tenant:USERINFO-B@example.invalid/data")
    for request in (first, second):
        del request.headers["Authorization"]

    assert _cache_key(first) != _cache_key(second)


def test_non_json_bodies_are_never_reparsed_as_json(tmp_path: Path) -> None:
    """A CSV export that happens to parse as JSON must survive the cache intact."""
    body = '{"token": "SYNTHETIC-CSV-VALUE"}'
    with SafeCachedSession(cache_path=tmp_path) as session:
        session.mount("https://", _SyntheticAdapter(body=body, content_type="text/csv"))
        live = session.get("https://example.invalid/export.csv")
        cached = session.get("https://example.invalid/export.csv")

    assert live.text == body
    assert cached.from_cache
    assert cached.text == body
