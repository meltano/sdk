from __future__ import annotations

from pathlib import Path

import requests

from singer_sdk.helpers.http_cache import SafeCachedSession


class _SyntheticAdapter(requests.adapters.BaseAdapter):
    def __init__(self, *, response_url: str | None = None) -> None:
        self.response_url = response_url
        self.seen_headers = {}
        self.seen_body = None

    def send(self, request, **kwargs):  # noqa: ARG002
        self.seen_headers = dict(request.headers)
        self.seen_body = request.body
        response = requests.Response()
        response.status_code = 200
        response.url = self.response_url or request.url
        response.request = request
        response.headers["Set-Cookie"] = "session=SYNTHETIC-COOKIE-SECRET"
        response._content = b'{"result": "synthetic"}'
        response.raw = requests.packages.urllib3.response.HTTPResponse(
            body=response._content,
            headers={"Set-Cookie": "session=SYNTHETIC-COOKIE-SECRET"},
            status=200,
            request_url=request.url,
        )
        return response

    def close(self) -> None:
        pass


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
            "https://example.invalid/data"
            "?access_token=SYNTHETIC-QUERY-SECRET&safe=yes",
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
    persisted = b"\n".join(
        path.read_bytes() for path in tmp_path.rglob("*") if path.is_file()
    ).lower()
    for marker in marker_values:
        assert marker.lower().encode() not in persisted
    for header in (b"authorization", b"private-token", b"set-cookie", b"cookie"):
        assert header not in persisted


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

    persisted = b"\n".join(
        path.read_bytes() for path in tmp_path.rglob("*") if path.is_file()
    )
    for marker, _ in markers_and_content_types:
        assert marker.encode() not in persisted


def test_default_cache_is_outside_current_directory(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.chdir(tmp_path)

    with SafeCachedSession() as session:
        assert not Path(session.cache.cache_dir).is_relative_to(tmp_path)
    assert not (tmp_path / ".http_cache").exists()
