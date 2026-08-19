"""Safe filesystem caching for SDK sample taps.

Requires the optional ``requests-cache`` dependency, available as
``singer-sdk[cache]``.
"""

from __future__ import annotations

import json
import typing as t
from hashlib import sha256
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import attr
from requests.cookies import RequestsCookieJar
from requests.structures import CaseInsensitiveDict
from requests_cache import CachedSession, create_key
from requests_cache.serializers import SerializerPipeline, Stage, json_serializer

if t.TYPE_CHECKING:
    from collections.abc import Iterable, Iterator, MutableMapping

    from requests import PreparedRequest, Request
    from requests_cache.models import CachedRequest, CachedResponse

    _AnyRequest = PreparedRequest | Request | CachedRequest

_HeaderValue = t.TypeVar("_HeaderValue")

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
# requests-cache matches `ignored_parameters` against header names *as sent*: its
# `filter_sort_dict` tests `name in ignored_parameters` against the original casing
# yielded by `CaseInsensitiveDict.items()`. List both the canonical HTTP casing and
# the lower-case (HTTP/2) form so its own redaction keeps working.
_IGNORED_PARAMETERS = sorted(
    _SENSITIVE_PARAMETERS
    | _SENSITIVE_HEADERS
    | {name.title() for name in _SENSITIVE_HEADERS}
)


def _decode_text(value: object) -> str | None:
    """Return ``value`` as text, or ``None`` when it is not safely decodable."""
    if isinstance(value, str):
        return value
    if isinstance(value, (bytes, bytearray)):
        try:
            return bytes(value).decode()
        except UnicodeDecodeError:
            return None
    return None


def _strip_headers(headers: MutableMapping[str, _HeaderValue]) -> None:
    for name in list(headers):
        if name.lower() in _SENSITIVE_HEADERS:
            del headers[name]


def _sanitised_headers(
    headers: MutableMapping[str, str] | None,
) -> CaseInsensitiveDict:
    stripped: CaseInsensitiveDict = CaseInsensitiveDict(headers or {})
    _strip_headers(stripped)
    return stripped


def _content_type(headers: MutableMapping[str, str] | None) -> str:
    raw = _decode_text(CaseInsensitiveDict(headers or {}).get("Content-Type")) or ""
    return raw.partition(";")[0].lower()


def _strip_url(url: str) -> str:
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


def _strip_form_body(body: str | None) -> str | None:
    if body is None:
        return None
    try:
        pairs = parse_qsl(body, keep_blank_values=True, strict_parsing=True)
    except ValueError:
        return None
    return urlencode([
        (name, value)
        for name, value in pairs
        if name.lower() not in _SENSITIVE_PARAMETERS
    ])


def _strip_json_body(body: str | None) -> str | None:
    if body is None:
        return None
    try:
        payload = json.loads(body)
    except ValueError:
        return None
    return json.dumps(_strip_json_credentials(payload))


def _sanitised_request(request: CachedRequest | None) -> CachedRequest | None:
    """Return a copy of ``request`` with every credential-bearing field removed."""
    if request is None:
        return None

    body: str | bytes | None = request.body
    if body:
        content_type = _content_type(request.headers)
        text = _decode_text(body)
        if content_type == "application/x-www-form-urlencoded":
            body = _strip_form_body(text)
        elif content_type == "application/json":
            body = _strip_json_body(text)
        else:
            # Do not guess whether opaque or unsupported bodies are credential-free.
            body = None

    return attr.evolve(
        request,
        body=body,
        cookies=RequestsCookieJar(),
        headers=_sanitised_headers(request.headers),
        url=_strip_url(request.url or ""),
    )


def _sanitised_cached_request(request: CachedRequest) -> CachedRequest:
    """Return a sanitised copy of a request that is always present.

    Returns:
        The sanitised request. ``_sanitised_request`` only returns ``None`` for a
        ``None`` input, so the result is never ``None`` here.
    """
    return t.cast("CachedRequest", _sanitised_request(request))


def _sanitised_content(response: CachedResponse) -> bytes:
    """Strip recognised credential fields from a JSON response body.

    Only JSON bodies are rewritten: other content types are the payload the cache
    exists to serve, and cannot be parsed for credentials without guessing.

    Returns:
        The body to persist, with recognised credential fields removed.
    """
    content: bytes = response.content
    if not content or _content_type(response.headers) != "application/json":
        return content
    stripped = _strip_json_body(_decode_text(content))
    return content if stripped is None else stripped.encode()


def sanitise_cached_response(response: CachedResponse) -> CachedResponse:
    """Return a sanitised copy of a response on its way into the cache.

    ``CachedResponse.from_response`` shares the live response's header mapping,
    cookie jar and request headers, so this copies instead of stripping in place:
    the caller keeps ``response.cookies``, and a 302 handed to
    ``Session.resolve_redirects`` keeps the ``Authorization`` for the next hop.

    Returns:
        A copy with credential-bearing persistence fields removed.
    """
    return attr.evolve(
        response,
        content=_sanitised_content(response),
        cookies=RequestsCookieJar(),
        headers=_sanitised_headers(response.headers),
        history=[sanitise_cached_response(item) for item in response.history],
        # ``response.next`` is a property that re-prepares a ``PreparedRequest``;
        # the ``_next`` field it derives from is the ``CachedRequest`` that is
        # actually persisted, and is what ``evolve`` writes here.
        next=_sanitised_request(response._next),  # noqa: SLF001
        request=_sanitised_cached_request(response.request),
        url=_strip_url(response.url),
    )


#: Every byte written to a cache file passes through the serializer, so registering
#: the sanitisation here - rather than as a session response hook, which
#: ``Session.send()`` skips for requests it did not prepare - leaves no bypass.
_SANITISING_JSON_SERIALIZER = SerializerPipeline(
    [
        Stage(dumps=sanitise_cached_response, loads=lambda value: value),
        *json_serializer.stages,
    ],
    name=json_serializer.name,
    is_binary=json_serializer.is_binary,
)


def _json_credentials(value: object) -> Iterator[tuple[str, str]]:
    if isinstance(value, dict):
        for key, item in value.items():
            if str(key).lower() in _SENSITIVE_PARAMETERS:
                yield str(key).lower(), json.dumps(item, sort_keys=True)
            else:
                yield from _json_credentials(item)
    elif isinstance(value, list):
        for item in value:
            yield from _json_credentials(item)


def _body_credentials(request: _AnyRequest) -> Iterator[tuple[str, str]]:
    body = _decode_text(getattr(request, "body", None))
    if not body:
        return
    content_type = _content_type(getattr(request, "headers", None))
    if content_type == "application/x-www-form-urlencoded":
        yield from (
            (name.lower(), value)
            for name, value in parse_qsl(body, keep_blank_values=True)
            if name.lower() in _SENSITIVE_PARAMETERS
        )
    elif content_type == "application/json":
        try:
            payload = json.loads(body)
        except ValueError:
            return
        yield from _json_credentials(payload)


def _credential_material(request: _AnyRequest) -> list[tuple[str, str]]:
    """Collect the real credential values a request carries.

    Returns:
        Sorted ``(name, value)`` pairs for every credential found.
    """
    headers: MutableMapping[str, str] = getattr(request, "headers", None) or {}
    material = [
        (name.lower(), _decode_text(headers[name]) or "")
        for name in headers
        if name.lower() in _SENSITIVE_HEADERS
    ]

    # Userinfo needs no entry here: requests-cache redacts *query parameters* named
    # in ``ignored_parameters``, never the netloc, so ``create_key`` already keeps
    # ``https://t:A@host`` and ``https://t:B@host`` on separate keys.
    parts = urlsplit(getattr(request, "url", None) or "")
    material.extend(
        (name.lower(), value)
        for name, value in parse_qsl(parts.query, keep_blank_values=True)
        if name.lower() in _SENSITIVE_PARAMETERS
    )
    material.extend(_body_credentials(request))
    return sorted(material)


def _cache_key(request: _AnyRequest, **kwargs: t.Any) -> str:
    """Build a cache key that distinguishes requests by their real credentials.

    requests-cache redacts ignored parameters *into* the key: it substitutes the
    literal ``REDACTED`` so "the cache key will still match whether the parameter
    was present or not", which collapses distinct tokens, and distinct pagination
    cursors named ``token``, onto one entry. Mixing in a non-reversible digest of
    the real values keeps them apart without persisting them.

    Returns:
        The requests-cache key, suffixed with a digest of the credentials.
    """
    digest = sha256()
    for name, value in _credential_material(request):
        digest.update(f"{name}={value}\n".encode())
    return f"{create_key(request=request, **kwargs)}{digest.hexdigest()[:16]}"


class SafeCachedSession(CachedSession):
    """A cache session whose default filesystem lives outside the repository."""

    def __init__(
        self,
        *,
        cache_name: str = "http-cache",
        cache_path: str | Path | None = None,
        allowable_methods: Iterable[str] = ("GET", "HEAD"),
    ) -> None:
        """Initialise a sanitising session with a stable per-tap cache path.

        Args:
            cache_name: Name of this tap's cache, used when ``cache_path`` is unset.
            cache_path: Explicit cache directory. A relative path is resolved inside
                the user cache directory.
            allowable_methods: HTTP methods whose responses may be cached.
        """
        super().__init__(
            cache_path if cache_path is not None else Path("singer-sdk") / cache_name,
            backend="filesystem",
            serializer=_SANITISING_JSON_SERIALIZER,
            use_cache_dir=True,
            allowable_methods=allowable_methods,
            ignored_parameters=_IGNORED_PARAMETERS,
            match_headers=True,
            key_fn=_cache_key,
        )
