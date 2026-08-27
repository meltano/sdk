"""Shared VCR-cassette configuration for taps/targets that hit real HTTP APIs in tests.

Pairs with `pytest-recording <https://github.com/kiwicom/pytest-recording>`_'s
``@pytest.mark.vcr`` and ``vcr_config`` fixture. Record real HTTP traffic once,
replay it deterministically afterward, with credentials scrubbed *before* a
cassette is ever written to disk.

Example:
    .. code-block:: python

        # conftest.py
        from pathlib import Path

        import pytest

        from singer_sdk.testing.vcr import default_vcr_config


        @pytest.fixture(scope="module")
        def vcr_config():
            return default_vcr_config(Path(__file__).parent / "cassettes")
"""

from __future__ import annotations

import json
import logging
import typing as t

if t.TYPE_CHECKING:
    from pathlib import Path

    import pytest
    from vcr.request import Request

# `vcrpy` logs each request/cassette interaction at INFO level under the `vcr`
# namespace. Left alone, that leaks into any test asserting on `caplog` output
# (e.g. snapshot-based log assertions), and differs between recording and replay.
# Harmless if `vcrpy`/`pytest-recording` isn't installed: this only names a logger,
# it doesn't import the package.
logging.getLogger("vcr").setLevel(logging.WARNING)

__all__ = [
    "SENSITIVE_HEADERS",
    "SENSITIVE_QUERY_PARAMS",
    "VCRConfig",
    "default_vcr_config",
    "scrub_request_headers",
    "scrub_response_body",
    "use_class_cassette",
]


class VCRConfig(t.TypedDict, total=False):
    """Shape of the dict returned by `default_vcr_config`.

    Matches the subset of `vcrpy`'s `VCR()` constructor kwargs that
    `pytest-recording`'s `vcr_config` fixture accepts. `total=False` since
    `cassette_library_dir` is only present when a `cassette_dir` is passed to
    `default_vcr_config`; callers merging in extras (e.g. `record_mode`,
    `match_on`) can widen the dict freely at the call site.
    """

    cassette_library_dir: str
    filter_headers: list[str]
    filter_query_parameters: list[str]
    before_record_request: t.Callable[[Request], Request]
    before_record_response: t.Callable[[dict[str, t.Any]], dict[str, t.Any]]


SENSITIVE_HEADERS = (
    "authorization",
    "cookie",
    "private-token",
    "proxy-authorization",
    "set-cookie",
    "x-api-key",
    "x-api-token",
    "x-auth-token",
)

SENSITIVE_QUERY_PARAMS = (
    "access_token",
    "api_key",
    "apikey",
    "auth",
    "authorization",
    "client_secret",
    "password",
    "private_token",
    "refresh_token",
    "signature",
    "token",
)

_SENSITIVE_BODY_KEYS = frozenset({
    *SENSITIVE_QUERY_PARAMS,
    "accesstoken",
    "refreshtoken",
})

_REDACTED = "REDACTED"

# Neither `requests` nor `vcrpy` replay uses stored request headers for anything
# (cassette matching is method/scheme/host/port/path/query by default), so they're
# pure noise in the cassette. Kept only for a human skimming the file.
_REQUEST_HEADER_KEEP = frozenset({"content-type"})

# Response headers *are* handed back to the tap, so anything a paginator or stream
# might read has to survive. `content-type` affects response parsing; the rest are a
# generic net for pagination-style headers (GitLab's `X-Next-Page`, RFC 5988 `Link`,
# etc.) so this stays useful for taps other than the bundled samples. Everything
# else (CDN/rate-limit/tracing headers: `CF-Ray`, `ratelimit-*`, `x-request-id`, ...)
# carries no information the tests rely on.
_RESPONSE_HEADER_KEEP_EXACT = frozenset({"content-type"})
# "limit"/"offset" are deliberately excluded even though some APIs page via plain
# `X-Limit`/`X-Offset` headers: including them here would also keep noisy
# `ratelimit-*`/`x-ratelimit-*` headers via substring match, which isn't worth it
# for the bundled sample taps (none of which page this way).
_RESPONSE_HEADER_KEEP_SUBSTRINGS = (
    "page",
    "link",
    "cursor",
    "next",
    "total",
)


def _is_kept_response_header(name: str) -> bool:
    lowered = name.lower()
    return lowered in _RESPONSE_HEADER_KEEP_EXACT or any(
        substring in lowered for substring in _RESPONSE_HEADER_KEEP_SUBSTRINGS
    )


def _scrub_json(value: object) -> object:
    if isinstance(value, dict):
        return {
            key: _REDACTED
            if str(key).lower() in _SENSITIVE_BODY_KEYS
            else _scrub_json(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_scrub_json(item) for item in value]
    return value


def scrub_request_headers(request: Request) -> Request:
    """Drop request headers that replay never reads, keeping the cassette lean.

    Intended for use as a `vcrpy` `before_record_request` hook. Cassette matching
    is method/scheme/host/port/path/query by default, so stored request headers
    are informational only; this keeps just `Content-Type`.

    Args:
        request: The `vcrpy` `Request` about to be recorded.

    Returns:
        The same request, with all but a minimal set of headers removed.
    """
    for name in list(request.headers):
        if name.lower() not in _REQUEST_HEADER_KEEP:
            del request.headers[name]
    return request


def scrub_response_body(response: dict[str, t.Any]) -> dict[str, t.Any]:
    """Strip credential-shaped fields from a cassette response before it's recorded.

    Intended for use as a `vcrpy` `before_record_response` hook. Drops response
    headers that no paginator or stream reads back (CDN/rate-limit/tracing noise),
    redacts anything sensitive that survives, and scrubs credential-shaped
    top-level and nested JSON body keys (e.g. `accessToken`, `refreshToken`,
    `password`).

    Args:
        response: The `vcrpy`-formatted response dict about to be serialised.

    Returns:
        The same response dict, with sensitive fields redacted in place.
    """
    headers = response.get("headers", {})
    for name in list(headers):
        if not _is_kept_response_header(name):
            del headers[name]
        elif name.lower() in SENSITIVE_HEADERS:
            headers[name] = [_REDACTED]

    body = response.get("body", {})
    string = body.get("string") if isinstance(body, dict) else None
    if not string:
        return response

    try:
        text = string.decode() if isinstance(string, bytes) else string
        parsed = json.loads(text)
    except (UnicodeDecodeError, ValueError):
        return response

    scrubbed = json.dumps(_scrub_json(parsed)).encode()
    body["string"] = scrubbed
    return response


def default_vcr_config(cassette_dir: str | Path | None = None) -> VCRConfig:
    """Build a `pytest-recording` `vcr_config` dict with credential scrubbing applied.

    Args:
        cassette_dir: Directory where cassette files are stored and replayed from.
            Leave unset to use `pytest-recording`'s own default, a `cassettes/`
            directory scoped per test *module* (avoids cross-module cassette name
            collisions, e.g. two unrelated `get_tap_test_class`-generated test
            classes both named `TapTestClass`).

    Returns:
        Kwargs for the `vcr_config` fixture: sensitive headers, query parameters and
        JSON body fields are redacted before anything is written to disk. Recording
        behavior (replay-only vs. record) is left to `pytest-recording`'s own
        `--record-mode` CLI option (defaults to replay-only, i.e. `none`), so this
        config never silently overrides a maintainer's explicit `--record-mode`.
    """
    config: VCRConfig = {
        "filter_headers": list(SENSITIVE_HEADERS),
        "filter_query_parameters": list(SENSITIVE_QUERY_PARAMS),
        "before_record_request": scrub_request_headers,
        "before_record_response": scrub_response_body,
    }
    if cassette_dir is not None:
        config["cassette_library_dir"] = str(cassette_dir)
    return config


def use_class_cassette(
    request: pytest.FixtureRequest,
    vcr_cassette_dir: str,
    config: VCRConfig | None = None,
) -> t.Iterator[None]:
    """Wrap a whole test class in one VCR cassette, keyed by a custom marker.

    `pytest-recording`'s own cassette fixture (`@pytest.mark.vcr`) is function
    scoped, but `get_tap_test_class`'s standard test suite fetches records via a
    *class*-scoped `runner` fixture (`singer_sdk/testing/factory.py`) that does the
    real HTTP sync once, on whichever test in the class runs first. pytest sets up
    broader-scoped fixtures before narrower-scoped ones, so that one-time sync
    always happens before `pytest-recording`'s function-scoped cassette fixture
    would activate — the sync is never actually intercepted, regardless of test
    execution order or which test happens to trigger it. Wire this into a
    class-scoped, autouse fixture instead, keyed off a `vcr_cassette` marker so it
    doesn't compete with `pytest-recording`'s own `vcr`/`default_cassette` markers:

    .. code-block:: python

        # conftest.py
        @pytest.fixture(scope="class", autouse=True)
        def _class_cassette(request, vcr_cassette_dir):
            yield from use_class_cassette(request, vcr_cassette_dir)


        # test_my_tap.py
        TestMyTap = pytest.mark.vcr_cassette("my_tap.yaml")(get_tap_test_class(...))

    Args:
        request: The current test's fixture request.
        vcr_cassette_dir: Directory cassettes are stored in, e.g. from
            `pytest-recording`'s own `vcr_cassette_dir` fixture.
        config: `vcr_config`-style overrides; defaults to `default_vcr_config()`.

    Yields:
        Nothing; only wraps the class in an active cassette for its duration.
    """
    marker = request.node.get_closest_marker("vcr_cassette")
    if marker is None:
        yield
        return

    import vcr as vcrpy  # noqa: PLC0415

    cassette_config: dict[str, t.Any] = dict(config or default_vcr_config())
    cassette_dir = cassette_config.pop("cassette_library_dir", None) or vcr_cassette_dir
    record_mode = request.config.getoption("--record-mode") or "none"
    class_vcr = vcrpy.VCR(
        cassette_library_dir=cassette_dir,
        record_mode=record_mode,
        **cassette_config,
    )
    with class_vcr.use_cassette(marker.args[0]):
        yield
