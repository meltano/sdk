from __future__ import annotations

from pathlib import Path

import requests

from singer_sdk.helpers.http_cache import SafeCachedSession


class _SyntheticAdapter(requests.adapters.BaseAdapter):
    def send(self, request, **kwargs):  # noqa: ARG002
        response = requests.Response()
        response.status_code = 200
        response.url = request.url
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
    }
    with SafeCachedSession(cache_path=tmp_path, allowable_methods=("POST",)) as session:
        session.mount("https://", _SyntheticAdapter())
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
    persisted = b"\n".join(
        path.read_bytes() for path in tmp_path.rglob("*") if path.is_file()
    ).lower()
    for marker in marker_values:
        assert marker.lower().encode() not in persisted
    for header in (b"authorization", b"private-token", b"set-cookie", b"cookie"):
        assert header not in persisted


def test_default_cache_is_outside_current_directory(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.chdir(tmp_path)

    with SafeCachedSession() as session:
        assert not Path(session.cache.cache_dir).is_relative_to(tmp_path)
    assert not (tmp_path / ".http_cache").exists()
