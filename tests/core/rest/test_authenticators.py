"""Tests for authentication helpers."""

from __future__ import annotations

import datetime
import os
import typing as t
import warnings

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
from requests.auth import HTTPProxyAuth, _basic_auth_str

from singer_sdk.authenticators import (
    APIKeyAuthenticator,
    BasicAuthenticator,
    BearerTokenAuthenticator,
    OAuthAuthenticator,
    OAuthJWTAuthenticator,
    SimpleAuthenticator,
)
from singer_sdk.helpers._compat import SingerSDKDeprecationWarning

if t.TYPE_CHECKING:
    import requests_mock
    from cryptography.hazmat.primitives.asymmetric.rsa import (
        RSAPrivateKey,
        RSAPublicKey,
    )

    from singer_sdk.streams import RESTStream
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
    stream: RESTStream = rest_tap.streams[stream_name]
    other_stream: RESTStream = rest_tap.streams[other_stream_name]

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
    stream: RESTStream = rest_tap.streams["proxy_auth_stream"]
    r = stream.prepare_request(None, None)

    assert isinstance(stream.authenticator, HTTPProxyAuth)
    assert r.headers["Proxy-Authorization"] == _basic_auth_str("username", "password")


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
    stream: RESTStream = rest_tap.streams["some_stream"]
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

    stream: RESTStream = rest_tap.streams["some_stream"]
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

    stream: RESTStream = rest_tap.streams["some_stream"]
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

    stream: RESTStream = rest_tap.streams["some_stream"]
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

    stream: RESTStream = rest_tap.streams["some_stream"]
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

    stream: RESTStream = rest_tap.streams["some_stream"]
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
    stream: RESTStream = rest_tap.streams["some_stream"]
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
    stream: RESTStream = rest_tap.streams["some_stream"]
    with pytest.deprecated_call() as recorder:
        BearerTokenAuthenticator.create_for_stream(stream, token="bearer-token")  # noqa: S106

    assert len(recorder.list) == 2  # Both create_for_stream and stream param warnings
    assert_create_for_stream_deprecation_warning(recorder.list[0])
    assert_stream_param_deprecation_warning(recorder.list[1])


def test_basic_authenticator_create_for_stream_deprecation_warning(rest_tap: Tap):
    """Validate that a warning is emitted when using create_for_stream."""
    stream: RESTStream = rest_tap.streams["some_stream"]
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
