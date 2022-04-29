"""Tests for authentication helpers."""

from __future__ import annotations

import jwt
import pytest
import requests_mock
from cryptography.hazmat.primitives.asymmetric.rsa import (
    RSAPrivateKey,
    RSAPublicKey,
    generate_private_key,
)
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    NoEncryption,
    PrivateFormat,
    PublicFormat,
)

from singer_sdk.authenticators import OAuthAuthenticator, OAuthJWTAuthenticator
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
    rest_tap: Tap, stream_name: str, other_stream_name: str, auth_reused: bool
):
    """Validate that the stream's authenticator is a singleton."""
    stream: RESTStream = rest_tap.streams[stream_name]
    other_stream: RESTStream = rest_tap.streams[other_stream_name]

    assert (stream.authenticator is other_stream.authenticator) is auth_reused


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
    result: bool,
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
    authenticator.update_access_token()

    assert authenticator.expires_in == result


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
        oauth_request_body = {"some": "payload"}

    authenticator = _FakeOAuthJWTAuthenticator(stream=rest_tap.streams["some_stream"])

    body = authenticator.oauth_request_body
    payload = authenticator.oauth_request_payload
    token = payload["assertion"]

    assert jwt.decode(token, public_key_string, algorithms=["RS256"]) == body
