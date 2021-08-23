"""Classes to assist in authenticating to APIs."""

import base64
import logging
import jwt
import math
import requests

from datetime import timedelta, datetime
from types import MappingProxyType
from typing import Any, Dict, Mapping, Optional, Union

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

from singer_sdk.helpers._util import utc_now
from singer_sdk.streams import Stream as RESTStreamBase

from singer import utils


class SingletonMeta(type):
    """A general purpose singleton metaclass."""

    def __init__(cls, name, bases, dic):
        """Init metaclass.

        The single instance is saved as an attribute of the the metaclass.
        """
        cls.__single_instance = None
        super().__init__(name, bases, dic)

    def __call__(cls, *args, **kwargs):
        """Create or reuse the singleton."""
        if cls.__single_instance:
            return cls.__single_instance
        single_obj = cls.__new__(cls)
        single_obj.__init__(*args, **kwargs)
        cls.__single_instance = single_obj
        return single_obj


class APIAuthenticatorBase:
    """Base class for offloading API auth."""

    def __init__(self, stream: RESTStreamBase):
        """Init authenticator."""
        self.tap_name: str = stream.tap_name
        self._config: Dict[str, Any] = dict(stream.config)
        self._auth_headers: Dict[str, Any] = {}
        self._auth_params: Dict[str, Any] = {}
        self.logger: logging.Logger = stream.logger

    @property
    def config(self) -> Mapping[str, Any]:
        """Return a frozen (read-only) config dictionary map."""
        return MappingProxyType(self._config)

    @property
    def auth_headers(self) -> dict:
        """Return http headers."""
        return self._auth_headers or {}

    @property
    def auth_params(self) -> dict:
        """Return query parameters."""
        return self._auth_params or {}


class SimpleAuthenticator(APIAuthenticatorBase):
    """DEPRECATED: Please use a more specific authenticator.

    This authenticator will merge a key-value pair to the stream
    in either the request headers or query parameters.
    """

    def __init__(self, stream: RESTStreamBase, auth_headers: Optional[Dict] = None):
        """Init authenticator.

        If auth_headers is provided, it will be merged with http_headers specified on
        the stream.
        """
        super().__init__(stream=stream)
        if self._auth_headers is None:
            self._auth_headers = {}
        if auth_headers:
            self._auth_headers.update(auth_headers)


class ApiKeyAuthenticator(APIAuthenticatorBase):
    """Implements API key authentication for REST Streams.

    This authenticator will merge a key-value pair with either the
    HTTP headers or query parameters specified on the stream. Common
    examples of key names are "x-api-key" and "Authorization" but
    any key-value pair may be used for this authenticator.
    """

    def __init__(
        self,
        stream: RESTStreamBase,
        key: str,
        value: str,
        location: str = "header",
    ):
        """Init authenticator."""
        super().__init__(stream=stream)
        auth_credentials = {key: value}

        if location not in ["header", "params"]:
            raise ValueError("`type` must be one of 'header' or 'params'.")

        if location == "header":
            if self._auth_headers is None:
                self._auth_headers = {}
            self._auth_headers.update(auth_credentials)
        elif location == "params":
            if self._auth_params is None:
                self._auth_params = {}
            self._auth_params.update(auth_credentials)

    @classmethod
    def create_for_stream(
        cls,
        stream: RESTStreamBase,
        key: str,
        value: str,
        location: str,
    ) -> "ApiKeyAuthenticator":
        """Create an Authenticator object specific to the Stream class."""
        return cls(stream=stream, key=key, value=value, location=location)


class BearerTokenAuthenticator(APIAuthenticatorBase):
    """Implements bearer token authentication for REST Streams.

    This Authenticator implements Bearer Token authentication. The token
    is a text string, included in the request header and prefixed with
    'Bearer '. The token will be merged with HTTP headers on the stream.
    """

    def __init__(self, stream: RESTStreamBase, token: str):
        """Init authenticator."""
        super().__init__(stream=stream)
        auth_credentials = {"Authorization": f"Bearer {token}"}

        if self._auth_headers is None:
            self._auth_headers = {}
        self._auth_headers.update(auth_credentials)

    @classmethod
    def create_for_stream(
        cls, stream: RESTStreamBase, token: str
    ) -> "BearerTokenAuthenticator":
        """Create an Authenticator object specific to the Stream class."""
        return cls(stream=stream, token=token)


class BasicAuthenticator(APIAuthenticatorBase):
    """Implements basic authentication for REST Streams.

    This Authenticator implements basic authentication by concatinating a
    username and password then base64 encoding the string. The resulting
    token will be merged with any HTTP headers specified on the stream.
    """

    def __init__(
        self,
        stream: RESTStreamBase,
        username: str,
        password: str,
    ):
        """Init authenticator."""
        super().__init__(stream=stream)
        credentials = f"{username}:{password}".encode()
        auth_token = base64.b64encode(credentials).decode("ascii")
        auth_credentials = {"Authorization": f"Basic {auth_token}"}

        if self._auth_headers is None:
            self._auth_headers = {}
        self._auth_headers.update(auth_credentials)

    @classmethod
    def create_for_stream(
        cls, stream: RESTStreamBase, username: str, password: str
    ) -> "BasicAuthenticator":
        """Create an Authenticator object specific to the Stream class."""
        return cls(stream=stream, username=username, password=password)


class OAuthAuthenticator(APIAuthenticatorBase):
    """API Authenticator for OAuth 2.0 flows."""

    def __init__(
        self,
        stream: RESTStreamBase,
        auth_endpoint: Optional[str] = None,
        oauth_scopes: Optional[str] = None,
    ) -> None:
        """Init authenticator."""
        super().__init__(stream=stream)
        self._auth_endpoint = auth_endpoint
        self._oauth_scopes = oauth_scopes

        # Initialize internal tracking attributes
        self.access_token: Optional[str] = None
        self.refresh_token: Optional[str] = None
        self.last_refreshed: Optional[datetime] = None
        self.expires_in: Optional[int] = None

    @property
    def auth_headers(self) -> dict:
        """Return a dictionary of auth headers to be applied.

        These will be merged with any `http_headers` specified in the stream.
        """
        if not self.is_token_valid():
            self.update_access_token()
        result = super().auth_headers
        result["Authorization"] = f"Bearer {self.access_token}"
        return result

    @property
    def auth_endpoint(self) -> str:
        """Return the authorization endpoint."""
        if not self._auth_endpoint:
            raise ValueError("Authorization endpoint not set.")
        return self._auth_endpoint

    @property
    def oauth_scopes(self) -> Optional[str]:
        """Return a string with the OAuth scopes, or None if not set."""
        return self._oauth_scopes

    @property
    def oauth_request_payload(self) -> dict:
        """Return the request body directly (OAuth) or encrypted (JWT)."""
        return self.oauth_request_body

    @property
    def oauth_request_body(self) -> dict:
        """Return formatted body of the OAuth authorization request.

        Sample implementation:

        ```py
        @property
        def oauth_request_body(self) -> dict:
            return {
                'grant_type': 'password',
                'scope': 'https://api.powerbi.com',
                'resource': 'https://analysis.windows.net/powerbi/api',
                'client_id': self.config["client_id"],
                'username': self.config.get("username", self.config["client_id"]),
                'password': self.config["password"],
            }
        ```
        """
        raise NotImplementedError(
            "The `oauth_request_body` property was not defined in the subclass."
        )

    @property
    def client_id(self) -> Optional[str]:
        """Return client ID string to be used in authentication or None if not set."""
        if self.config:
            return self.config.get("client_id")
        return None

    @property
    def client_secret(self) -> Optional[str]:
        """Return client secret to be used in authentication or None if not set."""
        if self.config:
            return self.config.get("client_secret")
        return None

    def is_token_valid(self) -> bool:
        """Return true if token is valid."""
        if self.last_refreshed is None:
            return False
        if not self.expires_in:
            return True
        if self.expires_in > (utils.now() - self.last_refreshed).total_seconds():
            return True
        return False

    # Authentication and refresh
    def update_access_token(self):
        """Update `access_token` along with: `last_refreshed` and `expires_in`."""
        request_time = utc_now()
        auth_request_payload = self.oauth_request_payload
        token_response = requests.post(self.auth_endpoint, data=auth_request_payload)
        try:
            token_response.raise_for_status()
            self.logger.info("OAuth authorization attempt was successful.")
        except Exception as ex:
            raise RuntimeError(
                f"Failed OAuth login, response was '{token_response.json()}'. {ex}"
            )
        token_json = token_response.json()
        self.access_token = token_json["access_token"]
        self.expires_in = token_json["expires_in"]
        self.last_refreshed = request_time


class OAuthJWTAuthenticator(OAuthAuthenticator):
    """API Authenticator for OAuth 2.0 flows which utilize a JWT refresh token."""

    @property
    def private_key(self) -> Optional[str]:
        """Return the private key to use in encryption."""
        return self.config.get("private_key", None)

    @property
    def private_key_passphrase(self) -> Optional[str]:
        """Return the private key passphrase to use in encryption."""
        return self.config.get("private_key_passphrase", None)

    @property
    def oauth_request_body(self) -> dict:
        """Return request body for OAuth request."""
        request_time = utc_now()
        return {
            "iss": self.client_id,
            "scope": self.oauth_scopes,
            "aud": self.auth_endpoint,
            "exp": math.floor((request_time + timedelta(hours=1)).timestamp()),
            "iat": math.floor(request_time.timestamp()),
        }

    @property
    def oauth_request_payload(self) -> dict:
        """Return request paytload for OAuth request."""
        if not self.private_key:
            raise ValueError("Missing 'private_key' property for OAuth payload.")

        private_key: Union[bytes, Any] = bytes(self.private_key, "UTF-8")
        if self.private_key_passphrase:
            passphrase = bytes(self.private_key_passphrase, "UTF-8")
            private_key = serialization.load_pem_private_key(
                private_key,
                password=passphrase,
                backend=default_backend(),
            )
        return {
            "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
            "assertion": jwt.encode(self.oauth_request_body, private_key, "RS256"),
        }
