"""Safe filesystem caching for SDK sample taps."""

from __future__ import annotations

import json
import typing as t
from tempfile import TemporaryDirectory
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from requests_cache import CachedSession

if t.TYPE_CHECKING:
    from collections.abc import Iterable, MutableMapping
    from pathlib import Path

    from requests import PreparedRequest, Response

_SENSITIVE_HEADERS = frozenset({
    "authorization",
    "cookie",
    "private-token",
    "proxy-authorization",
    "set-cookie",
    "x-access-token",
    "x-api-key",
    "x-api-token",
    "x-auth-token",
})
_SENSITIVE_PARAMETERS = frozenset({
    "access_token",
    "api_key",
    "apikey",
    "auth",
    "authorization",
    "client_secret",
    "cookie",
    "password",
    "private_token",
    "refresh_token",
    "signature",
    "token",
    "x-amz-signature",
})


def _strip_headers(headers: MutableMapping[str, object] | None) -> None:
    if headers is None:
        return
    for name in list(headers):
        if name.lower() in _SENSITIVE_HEADERS:
            del headers[name]


def _strip_url(url: str | None) -> str | None:
    if not url:
        return url
    parts = urlsplit(url)
    query = urlencode(
        [
            (name, value)
            for name, value in parse_qsl(parts.query, keep_blank_values=True)
            if name.lower() not in _SENSITIVE_PARAMETERS
        ],
        doseq=True,
    )
    # urlsplit().hostname drops userinfo but normalising the host can alter IPv6;
    # removing everything through the last `@` preserves the host and port exactly.
    netloc = parts.netloc.rsplit("@", maxsplit=1)[-1]
    return urlunsplit((parts.scheme, netloc, parts.path, query, parts.fragment))


def _strip_json_credentials(value: object) -> object:
    if isinstance(value, dict):
        return {
            key: _strip_json_credentials(item)
            for key, item in value.items()
            if str(key).lower() not in _SENSITIVE_PARAMETERS
        }
    if isinstance(value, list):
        return [_strip_json_credentials(item) for item in value]
    return value


def _strip_request(request: PreparedRequest | None) -> None:
    if request is None:
        return
    _strip_headers(request.headers)
    request.url = _strip_url(request.url)
    cookies = getattr(request, "_cookies", None)
    if cookies is not None:
        cookies.clear()

    content_type = request.headers.get("Content-Type", "").partition(";")[0].lower()
    if not request.body:
        return
    if content_type == "application/x-www-form-urlencoded":
        try:
            body = (
                request.body.decode()
                if isinstance(request.body, bytes)
                else request.body
            )
            request.body = urlencode([
                (name, value)
                for name, value in parse_qsl(
                    body,
                    keep_blank_values=True,
                    strict_parsing=True,
                )
                if name.lower() not in _SENSITIVE_PARAMETERS
            ])
        except (AttributeError, TypeError, UnicodeDecodeError, ValueError):
            request.body = None
    elif content_type == "application/json":
        try:
            body = (
                request.body.decode()
                if isinstance(request.body, bytes)
                else request.body
            )
            request.body = json.dumps(_strip_json_credentials(json.loads(body)))
        except (TypeError, UnicodeDecodeError, json.JSONDecodeError):
            request.body = None
    else:
        # Do not guess whether opaque or unsupported bodies are credential-free.
        request.body = None


def sanitise_response_for_cache(
    response: Response,
    *args: object,
    **kwargs: object,
) -> Response:
    """Remove authentication material before requests-cache serialises a response.

    Returns:
        The response with credential-bearing persistence fields removed.
    """
    del args, kwargs
    for item in [*response.history, response]:
        _strip_headers(item.headers)
        _strip_request(item.request)
        item.url = _strip_url(item.url)
        item.cookies.clear()

        raw = item.raw
        _strip_headers(getattr(raw, "headers", None))
        if hasattr(raw, "_request_url"):
            raw._request_url = _strip_url(raw._request_url)  # noqa: SLF001

    _strip_request(response.next)
    return response


class SafeCachedSession(CachedSession):
    """A cache session whose default filesystem lives outside the repository."""

    def __init__(
        self,
        *,
        cache_path: str | Path | None = None,
        allowable_methods: Iterable[str] = ("GET", "HEAD"),
    ) -> None:
        """Initialise a sanitising session with a temporary default cache path."""
        self._temporary_cache: TemporaryDirectory[str] | None = None
        if cache_path is None:
            self._temporary_cache = TemporaryDirectory(prefix="singer-sdk-http-cache-")
            cache_path = self._temporary_cache.name
        super().__init__(
            cache_path,
            backend="filesystem",
            serializer="json",
            allowable_methods=allowable_methods,
            ignored_parameters=sorted(_SENSITIVE_HEADERS | _SENSITIVE_PARAMETERS),
            match_headers=True,
        )
        self.hooks["response"].append(sanitise_response_for_cache)

    def close(self) -> None:
        """Close the cache backend and remove an automatically-created cache."""
        try:
            super().close()
        finally:
            if self._temporary_cache is not None:
                self._temporary_cache.cleanup()
                self._temporary_cache = None
