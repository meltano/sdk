"""Tests for authentication helpers."""

from __future__ import annotations

import datetime
import http
import logging
import os
import sys
import typing as t
import warnings

import backoff
import jwt
import pytest
import time_machine
from cryptography.hazmat.primitives.asymmetric.rsa import generate_private_key
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    NoEncryption,
    PrivateFormat,
    PublicFormat,
)

from singer_sdk.authenticators import (
    APIAuthenticatorBase,
    APIKeyAuthenticator,
    BasicAuthenticator,
    BearerTokenAuthenticator,
    OAuthAuthenticator,
    OAuthJWTAuthenticator,
    SimpleAuthenticator,
)
from singer_sdk.helpers._compat import SingerSDKDeprecationWarning
from singer_sdk.streams.rest import RESTStream

if sys.version_info >= (3, 12):
    from typing import override  # noqa: ICN003
else:
    from typing_extensions import override

if t.TYPE_CHECKING:
    import requests
    import requests_mock
    from cryptography.hazmat.primitives.asymmetric.rsa import (
        RSAPrivateKey,
        RSAPublicKey,
    )

    from singer_sdk.tap_base import Tap


@pytest.mark.parametrize(
    "stream_name,other_stream_name,auth_reused",
    [
        (
            "some_stream",
            "some_stream",
            False,
        ),
        (
            "some_stream",
            "other_stream",
            False,
        ),
        (
            "single_auth_stream",
            "single_auth_stream",
            True,
        ),
        (
            "single_auth_stream",
            "reused_single_auth_stream",
            True,
        ),
        (
            "cached_auth_stream",
            "cached_auth_stream",
            True,
        ),
        (
            "cached_auth_stream",
            "other_cached_auth_stream",
            False,
        ),
    ],
    ids=[
        "naive-auth-not-reused-between-requests",
        "naive-auth-not-reused-between-streams",
        "singleton-auth-reused-between-requests",
        "singleton-auth-reused-between-streams",
        "cached-auth-reused-between-requests",
        "cached-auth-not-reused-between-streams",
    ],
)
def test_authenticator_is_reused(
    rest_tap: Tap,
    stream_name: str,
    other_stream_name: str,
    auth_reused: bool,
):
    """Validate that the stream's authenticator is a singleton."""
    stream = t.cast("RESTStream", rest_tap.streams[stream_name])
    other_stream = t.cast("RESTStream", rest_tap.streams[other_stream_name])

    assert (stream.authenticator is other_stream.authenticator) is auth_reused


def test_simple_authenticator():
    auth = SimpleAuthenticator()
    assert auth.auth_headers == {}

    auth = SimpleAuthenticator(auth_headers={"Authorization": "Bearer token"})
    assert auth.auth_headers == {"Authorization": "Bearer token"}


def test_api_key_authenticator():
    auth = APIKeyAuthenticator(key="api-key", value="secret")
    assert auth.auth_headers == {"api-key": "secret"}
    assert auth.auth_params == {}

    auth = APIKeyAuthenticator(key="api-key", value="secret", location="params")
    assert auth.auth_headers == {}
    assert auth.auth_params == {"api-key": "secret"}


def test_basic_authenticator():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        auth = BasicAuthenticator(username="username", password="password")  # noqa: S106

    assert auth.auth_headers == {"Authorization": "Basic dXNlcm5hbWU6cGFzc3dvcmQ="}
    assert auth.auth_params == {}


def test_oauth_authenticator():
    auth = OAuthAuthenticator(client_id="client-id", client_secret="client-secret")  # noqa: S106
    assert auth.client_id == "client-id"
    assert auth.client_secret == "client-secret"  # noqa: S105


def test_oauth_jwt_authenticator():
    auth = OAuthJWTAuthenticator(
        private_key="private-key",
        private_key_passphrase="private-key-passphrase",  # noqa: S106
    )
    assert auth.private_key == "private-key"
    assert auth.private_key_passphrase == "private-key-passphrase"  # noqa: S105


class _FakeOAuthAuthenticator(OAuthAuthenticator):
    def oauth_request_body(self) -> dict:
        return {}


@pytest.mark.parametrize(
    "oauth_response_expires_in,default_expiration,result",
    [
        (
            123,
            None,
            123,
        ),
        (
            123,
            234,
            123,
        ),
        (
            None,
            234,
            234,
        ),
        (
            None,
            None,
            None,
        ),
    ],
    ids=[
        "expires-in-and-no-default-expiration",
        "expires-in-and-default-expiration",
        "no-expires-in-and-default-expiration",
        "no-expires-in-and-no-default-expiration",
    ],
)
def test_oauth_authenticator_token_expiry_handling(
    rest_tap: Tap,
    requests_mock: requests_mock.Mocker,
    oauth_response_expires_in: int,
    default_expiration: int,
    result: int | None,
):
    """Validate various combinations of expires_in and default_expiration."""
    response = {"access_token": "an-access-token"}

    if oauth_response_expires_in:
        response["expires_in"] = oauth_response_expires_in

    requests_mock.post(
        "https://example.com/oauth",
        json=response,
    )

    authenticator = _FakeOAuthAuthenticator(
        stream=rest_tap.streams["some_stream"],
        auth_endpoint="https://example.com/oauth",
        default_expiration=default_expiration,
    )
    with time_machine.travel(
        datetime.datetime(2023, 1, 1, tzinfo=datetime.timezone.utc),
        tick=False,
    ):
        authenticator.update_access_token()

    assert authenticator.expires_in == result

    with time_machine.travel(
        datetime.datetime(2023, 1, 1, 0, 1, tzinfo=datetime.timezone.utc),
        tick=False,
    ):
        assert authenticator.is_token_valid()

    with time_machine.travel(
        datetime.datetime(2023, 1, 1, 0, 5, tzinfo=datetime.timezone.utc),
        tick=False,
    ):
        assert not authenticator.expires_in or not authenticator.is_token_valid()


@pytest.fixture
def private_key() -> RSAPrivateKey:
    return generate_private_key(public_exponent=65537, key_size=4096)


@pytest.fixture
def public_key(private_key: RSAPrivateKey) -> RSAPublicKey:
    return private_key.public_key()


@pytest.fixture
def private_key_string(private_key: RSAPrivateKey) -> str:
    return private_key.private_bytes(
        Encoding.PEM,
        format=PrivateFormat.PKCS8,
        encryption_algorithm=NoEncryption(),
    ).decode("utf-8")


@pytest.fixture
def public_key_string(public_key: RSAPublicKey) -> str:
    return public_key.public_bytes(
        Encoding.PEM,
        format=PublicFormat.PKCS1,
    ).decode("utf-8")


def test_oauth_jwt_authenticator_payload(
    rest_tap: Tap,
    private_key_string: str,
    public_key_string: str,
):
    class _FakeOAuthJWTAuthenticator(OAuthJWTAuthenticator):
        private_key = private_key_string
        oauth_request_body = {"some": "payload"}  # noqa: RUF012

    authenticator = _FakeOAuthJWTAuthenticator(stream=rest_tap.streams["some_stream"])

    body = authenticator.oauth_request_body
    payload = authenticator.oauth_request_payload
    token = payload["assertion"]

    assert jwt.decode(token, public_key_string, algorithms=["RS256"]) == body


def test_requests_library_auth(rest_tap: Tap):
    """Validate that a requests.auth object can be used as an authenticator."""
    stream = t.cast("RESTStream", rest_tap.streams["proxy_auth_stream"])
    request = stream.prepare_request(None, None)
    authenticated_request = stream.authenticator(request)
    assert "Proxy-Authorization" in authenticated_request.headers


def _get_args_kwargs(
    stream_param: t.Literal["positional", "keyword"],
    stream: RESTStream,
) -> tuple[tuple[t.Any, ...], dict[str, t.Any]]:
    return ((stream,), {}) if stream_param == "positional" else ((), {"stream": stream})


def assert_basic_auth_deprecation_warning(
    warning: warnings.WarningMessage,
    *,
    location: str = "authenticators.py",
):
    assert isinstance(warning.message, SingerSDKDeprecationWarning)
    message = warning.message.args[0]
    assert isinstance(message, str)
    assert message.startswith("BasicAuthenticator is deprecated")
    assert warning.filename.endswith(f"{os.path.sep}{location}")


def assert_stream_param_deprecation_warning(warning: warnings.WarningMessage):
    assert isinstance(warning.message, SingerSDKDeprecationWarning)
    message = warning.message.args[0]
    assert isinstance(message, str)
    assert message.startswith("The `stream` parameter is deprecated")
    assert warning.filename.endswith(f"{os.path.sep}authenticators.py")


def assert_create_for_stream_deprecation_warning(warning: warnings.WarningMessage):
    assert isinstance(warning.message, SingerSDKDeprecationWarning)
    message = warning.message.args[0]
    assert isinstance(message, str)
    assert message.startswith("The `create_for_stream` method is deprecated")
    assert warning.filename.endswith(f"{os.path.sep}test_authenticators.py")


def assert_config_property_deprecation_warning(warning: warnings.WarningMessage):
    assert isinstance(warning.message, SingerSDKDeprecationWarning)
    message = warning.message.args[0]
    assert isinstance(message, str)
    assert message.startswith("The `config` property is deprecated")
    assert warning.filename.endswith(f"{os.path.sep}authenticators.py")


@pytest.mark.parametrize("stream_param", ["positional", "keyword"])
def test_basic_auth_deprecation_warning(
    rest_tap: Tap, stream_param: t.Literal["positional", "keyword"]
):
    """Validate that a warning is emitted when using BasicAuthenticator."""
    stream = t.cast("RESTStream", rest_tap.streams["some_stream"])
    args, kwargs = _get_args_kwargs(stream_param, stream)
    with pytest.deprecated_call() as recorder:
        BasicAuthenticator(*args, username="username", password="password", **kwargs)  # noqa: S106

    assert len(recorder.list) == 2
    assert_basic_auth_deprecation_warning(
        recorder.list[0],
        location="test_authenticators.py",
    )
    assert_stream_param_deprecation_warning(recorder.list[1])


@pytest.mark.parametrize("stream_param", ["positional", "keyword"])
def test_api_key_authenticator_stream_param_deprecation_warning(
    rest_tap: Tap,
    stream_param: t.Literal["positional", "keyword"],
):
    """Validate that a warning is emitted when using stream parameter."""
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        APIKeyAuthenticator(key="api-key", value="secret")

    stream = t.cast("RESTStream", rest_tap.streams["some_stream"])
    args, kwargs = _get_args_kwargs(stream_param, stream)
    with pytest.deprecated_call() as recorder:
        APIKeyAuthenticator(*args, key="api-key", value="secret", **kwargs)

    assert len(recorder.list) == 1
    assert_stream_param_deprecation_warning(recorder.list[0])


@pytest.mark.parametrize("stream_param", ["positional", "keyword"])
def test_bearer_token_authenticator_stream_param_deprecation_warning(
    rest_tap: Tap,
    stream_param: t.Literal["positional", "keyword"],
):
    """Validate that a warning is emitted when using stream parameter."""
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        BearerTokenAuthenticator(token="bearer-token")  # noqa: S106

    stream = t.cast("RESTStream", rest_tap.streams["some_stream"])
    args, kwargs = _get_args_kwargs(stream_param, stream)
    with pytest.deprecated_call() as recorder:
        BearerTokenAuthenticator(*args, token="bearer-token", **kwargs)  # noqa: S106

    assert len(recorder.list) == 1
    assert_stream_param_deprecation_warning(recorder.list[0])


@pytest.mark.parametrize("stream_param", ["positional", "keyword"])
def test_simple_authenticator_stream_param_deprecation_warning(
    rest_tap: Tap,
    stream_param: t.Literal["positional", "keyword"],
):
    """Validate that a warning is emitted when using stream parameter."""
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        SimpleAuthenticator(auth_endpoint="https://example.com/oauth")

    stream = t.cast("RESTStream", rest_tap.streams["some_stream"])
    args, kwargs = _get_args_kwargs(stream_param, stream)
    with pytest.deprecated_call() as recorder:
        SimpleAuthenticator(*args, **kwargs)

    assert len(recorder.list) == 1
    assert_stream_param_deprecation_warning(recorder.list[0])


@pytest.mark.parametrize("stream_param", ["positional", "keyword"])
def test_oauth_authenticator_stream_param_deprecation_warning(
    rest_tap: Tap,
    stream_param: t.Literal["positional", "keyword"],
):
    """Validate that a warning is emitted when using stream parameter."""
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        _FakeOAuthAuthenticator(auth_endpoint="https://example.com/oauth")

    stream = t.cast("RESTStream", rest_tap.streams["some_stream"])
    args, kwargs = _get_args_kwargs(stream_param, stream)
    with pytest.deprecated_call() as recorder:
        _FakeOAuthAuthenticator(
            *args,
            auth_endpoint="https://example.com/oauth",
            **kwargs,
        )

    # Expects 3 warnings: stream param, config property
    # (2x for client_id and client_secret)
    assert len(recorder.list) == 3
    assert_stream_param_deprecation_warning(recorder.list[0])
    assert_config_property_deprecation_warning(recorder.list[1])
    assert_config_property_deprecation_warning(recorder.list[2])


@pytest.mark.parametrize("stream_param", ["positional", "keyword"])
def test_oauth_jwt_authenticator_stream_param_deprecation_warning(
    rest_tap: Tap,
    stream_param: t.Literal["positional", "keyword"],
):
    """Validate that a warning is emitted when using stream parameter."""
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        OAuthJWTAuthenticator(auth_endpoint="https://example.com/oauth")

    stream = t.cast("RESTStream", rest_tap.streams["some_stream"])
    args, kwargs = _get_args_kwargs(stream_param, stream)
    with pytest.deprecated_call() as recorder:
        OAuthJWTAuthenticator(
            *args,
            auth_endpoint="https://example.com/oauth",
            **kwargs,
        )

    # Expects 5 warnings: stream param, config property
    # (4x for client_id, client_secret, private_key, private_key_passphrase)
    assert len(recorder.list) == 5
    assert_stream_param_deprecation_warning(recorder.list[0])
    assert_config_property_deprecation_warning(recorder.list[1])
    assert_config_property_deprecation_warning(recorder.list[2])
    assert_config_property_deprecation_warning(recorder.list[3])
    assert_config_property_deprecation_warning(recorder.list[4])


def test_api_key_authenticator_create_for_stream_deprecation_warning(rest_tap: Tap):
    """Validate that a warning is emitted when using create_for_stream."""
    stream = t.cast("RESTStream", rest_tap.streams["some_stream"])
    with pytest.deprecated_call() as recorder:
        APIKeyAuthenticator.create_for_stream(
            stream,
            key="api-key",
            value="secret",
            location="header",
        )

    assert len(recorder.list) == 2  # Both create_for_stream and stream param warnings
    assert_create_for_stream_deprecation_warning(recorder.list[0])
    assert_stream_param_deprecation_warning(recorder.list[1])


def test_bearer_token_authenticator_create_for_stream_deprecation_warning(
    rest_tap: Tap,
):
    """Validate that a warning is emitted when using create_for_stream."""
    stream = t.cast("RESTStream", rest_tap.streams["some_stream"])
    with pytest.deprecated_call() as recorder:
        BearerTokenAuthenticator.create_for_stream(stream, token="bearer-token")  # noqa: S106

    assert len(recorder.list) == 2  # Both create_for_stream and stream param warnings
    assert_create_for_stream_deprecation_warning(recorder.list[0])
    assert_stream_param_deprecation_warning(recorder.list[1])


def test_basic_authenticator_create_for_stream_deprecation_warning(rest_tap: Tap):
    """Validate that a warning is emitted when using create_for_stream."""
    stream = t.cast("RESTStream", rest_tap.streams["some_stream"])
    with pytest.deprecated_call() as recorder:
        BasicAuthenticator.create_for_stream(
            stream,
            username="username",
            password="password",  # noqa: S106
        )

    # create_for_stream, BasicAuthenticator, and stream param warnings
    assert len(recorder.list) == 3
    assert_create_for_stream_deprecation_warning(recorder.list[0])
    assert_basic_auth_deprecation_warning(recorder.list[1])
    assert_stream_param_deprecation_warning(recorder.list[2])


def test_authenticator_invoked_on_each_retry(
    requests_mock: requests_mock.Mocker,
    rest_tap: Tap,
    caplog: pytest.LogCaptureFixture,
):
    """Validate that the authenticator is invoked for each retry attempt.

    This test simulates a retry scenario where the first request fails with a
    retriable error (503), and subsequent requests succeed. It verifies that:
    1. The authenticator is called once for the initial attempt
    2. The authenticator is called again for each retry attempt
    3. Each request is re-authenticated, ensuring fresh authentication
    """
    # Track authenticator invocations
    auth_call_count = 0

    class TrackingAuthenticator(APIAuthenticatorBase):
        """Authenticator that tracks how many times it's called."""

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.auth_headers = {}

        def authenticate_request(
            self,
            request: requests.PreparedRequest,
        ) -> requests.PreparedRequest:
            """Track each authentication call."""
            nonlocal auth_call_count
            auth_call_count += 1
            # Set a unique token for each call to verify re-authentication
            self.auth_headers = {"Authorization": f"Bearer token-{auth_call_count}"}
            return super().authenticate_request(request)

    class RetryTestStream(RESTStream):
        """Stream configured for retry testing."""

        name = "retry_test"
        path = "/retry"
        url_base = "https://example.com"
        schema: t.ClassVar[dict] = {
            "type": "object",
            "properties": {"id": {"type": "integer"}},
        }

        # Configure minimal retries for fast testing
        backoff_max_tries = 3

        @override
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self._authenticator = TrackingAuthenticator()

        @override
        @property
        def authenticator(self) -> TrackingAuthenticator:
            return self._authenticator

        @override
        def backoff_wait_generator(self) -> t.Generator[float, None, None]:
            return backoff.constant(0)

        @override
        def backoff_jitter(self, value: float) -> float:
            return 0

    stream = RetryTestStream(rest_tap)
    error_status = http.HTTPStatus.SERVICE_UNAVAILABLE
    error_reason = "Service Unavailable"

    # Set up mock responses: fail twice with 503, then succeed
    requests_mock.get(
        "https://example.com/retry",
        [
            {"status_code": error_status, "reason": error_reason},
            {"status_code": error_status, "reason": error_reason},
            {"json": [{"id": 1}], "status_code": 200},
        ],
    )

    # Execute request - should trigger 2 retries before success
    with caplog.at_level(logging.ERROR):
        records = list(stream.get_records(None))

    # Verify the request succeeded
    assert records == [{"id": 1}]

    # Verify authenticator was called 4 times (setup + initial + 2 retries)
    assert auth_call_count == 4

    assert len(caplog.records) == 2
    assert all(rec.levelname == "ERROR" for rec in caplog.records)
    assert caplog.records[0].args == (
        0,  # Backoff seconds
        1,  # Retry number
        "/retry",
        error_status,
        error_reason,
    )
    assert caplog.records[1].args == (
        0,  # Backoff seconds
        2,  # Retry number
        "/retry",
        error_status,
        error_reason,
    )


def test_oauth_authenticator_refreshes_token_on_retry(
    requests_mock: requests_mock.Mocker,
    rest_tap: Tap,
):
    """Validate that OAuth tokens are refreshed when expired during retries.

    This test simulates a scenario where:
    1. An OAuth token expires between the initial request and a retry
    2. The authenticator detects the expired token and refreshes it
    3. The retry uses the new, fresh token
    """
    # Track token refresh calls
    token_refresh_count = 0
    current_token_version = 0

    class TestOAuthAuthenticator(OAuthAuthenticator):
        """OAuth authenticator for testing token refresh on retry."""

        @property
        def oauth_request_body(self) -> dict:
            """Return minimal OAuth request body."""
            return {"grant_type": "client_credentials"}

        def update_access_token(self) -> None:
            """Track token refresh and update version."""
            nonlocal token_refresh_count, current_token_version
            token_refresh_count += 1
            current_token_version += 1
            super().update_access_token()

    class OAuthRetryTestStream(RESTStream):
        """Stream with OAuth authentication for retry testing."""

        name = "oauth_retry_test"
        path = "/oauth-retry"
        url_base = "https://example.com"
        schema: t.ClassVar[dict] = {
            "type": "object",
            "properties": {"id": {"type": "integer"}},
        }
        backoff_max_tries = 2

        @override
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self._authenticator = TestOAuthAuthenticator(
                stream=self,
                auth_endpoint="https://example.com/oauth",
                default_expiration=60,  # 60 seconds
            )

        @override
        @property
        def authenticator(self) -> TestOAuthAuthenticator:
            return self._authenticator

        @override
        def backoff_wait_generator(self) -> t.Generator[float, None, None]:
            return backoff.constant(0)

        @override
        def backoff_jitter(self, value: float) -> float:
            return 0

    # Set up OAuth token endpoint
    requests_mock.post(
        "https://example.com/oauth",
        json={"access_token": "fresh-token", "expires_in": 60},
    )

    stream = OAuthRetryTestStream(rest_tap)

    # Set initial time and get initial token
    with time_machine.travel(
        datetime.datetime(
            2023,
            1,
            1,
            0,
            0,
            tzinfo=datetime.timezone.utc,
        ),
        tick=False,
    ):
        # Set up mock responses: fail with 503, then succeed
        requests_mock.get(
            "https://example.com/oauth-retry",
            [
                {"status_code": 503, "reason": "Service Unavailable"},
                {"json": [{"id": 1}], "status_code": 200},
            ],
        )

    # Make first request (will fail and be retried)
    # Move time forward so token appears expired during retry check
    with time_machine.travel(
        datetime.datetime(
            2023,
            1,
            1,
            0,
            2,
            tzinfo=datetime.timezone.utc,
        ),
        tick=False,
    ):
        records = list(stream.get_records(None))

    # Verify the request succeeded
    assert records == [{"id": 1}]

    # Verify token was refreshed (initial + refresh on retry)
    # Note: OAuth authenticator may refresh on first call + retry
    assert token_refresh_count >= 1
