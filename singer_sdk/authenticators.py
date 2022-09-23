"""Classes to assist in authenticating to APIs."""

from __future__ import annotations

import base64
import logging
import math
from datetime import datetime, timedelta
from types import MappingProxyType
from typing import Any, Mapping

import jwt
import requests
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

from singer_sdk.helpers._util import utc_now
from singer_sdk.streams import Stream as RESTStreamBase


class SingletonMeta(type):
    """A general purpose singleton metaclass."""

    def __init__(cls, name: str, bases: tuple[type], dic: dict) -> None:
        """Init metaclass.

        The single instance is saved as an attribute of the the metaclass.

        Args:
            name: Name of the derived class.
            bases: Base types of the derived class.
            dic: Class dictionary of the derived class.
        """
        cls.__single_instance = None
        super().__init__(name, bases, dic)

    def __call__(cls, *args: Any, **kwargs: Any) -> Any:  # noqa: ANN401
        """Create or reuse the singleton.

        Args:
            args: Class constructor positional arguments.
            kwargs: Class constructor keyword arguments.

        Returns:
            A singleton instance of the derived class.
        """
        if cls.__single_instance:
            return cls.__single_instance
        single_obj = cls.__new__(cls, None)  # type: ignore
        single_obj.__init__(*args, **kwargs)
        cls.__single_instance = single_obj
        return single_obj


class APIAuthenticatorBase:
    """Base class for offloading API auth."""

    def __init__(self, stream: RESTStreamBase) -> None:
        """Init authenticator.

        Args:
            stream: A stream for a RESTful endpoint.
        """
        self.tap_name: str = stream.tap_name
        self._config: dict[str, Any] = dict(stream.config)
        self._auth_headers: dict[str, Any] = {}
        self._auth_params: dict[str, Any] = {}
        self.logger: logging.Logger = stream.logger

    @property
    def config(self) -> Mapping[str, Any]:
        """Get stream or tap config.

        Returns:
            A frozen (read-only) config dictionary map.
        """
        return MappingProxyType(self._config)

    @property
    def auth_headers(self) -> dict:
        """Get headers.

        Returns:
            HTTP headers for authentication.
        """
        return self._auth_headers or {}

    @property
    def auth_params(self) -> dict:
        """Get query parameters.

        Returns:
            URL query parameters for authentication.
        """
        return self._auth_params or {}

    def authenticate_request(self, request: requests.Request) -> None:
        """Authenticate a request.

        Args:
            request: A `request object`_.

        .. _request object:
            https://requests.readthedocs.io/en/latest/api/#requests.Request
        """
        request.headers.update(self.auth_headers)
        request.params.update(self.auth_params)


class SimpleAuthenticator(APIAuthenticatorBase):
    """DEPRECATED: Please use a more specific authenticator.

    This authenticator will merge a key-value pair to the stream
    in either the request headers or query parameters.
    """

    def __init__(
        self,
        stream: RESTStreamBase,
        auth_headers: dict | None = None,
    ) -> None:
        """Create a new authenticator.

        If auth_headers is provided, it will be merged with http_headers specified on
        the stream.

        Args:
            stream: The stream instance to use with this authenticator.
            auth_headers: Authentication headers.
        """
        super().__init__(stream=stream)
        if self._auth_headers is None:
            self._auth_headers = {}
        if auth_headers:
            self._auth_headers.update(auth_headers)


class APIKeyAuthenticator(APIAuthenticatorBase):
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
    ) -> None:
        """Create a new authenticator.

        Args:
            stream: The stream instance to use with this authenticator.
            key: API key parameter name.
            value: API key value.
            location: Where the API key is to be added. Either 'header' or 'params'.

        Raises:
            ValueError: If the location value is not 'header' or 'params'.
        """
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
        cls: type[APIKeyAuthenticator],
        stream: RESTStreamBase,
        key: str,
        value: str,
        location: str,
    ) -> APIKeyAuthenticator:
        """Create an Authenticator object specific to the Stream class.

        Args:
            stream: The stream instance to use with this authenticator.
            key: API key parameter name.
            value: API key value.
            location: Where the API key is to be added. Either 'header' or 'params'.

        Returns:
            APIKeyAuthenticator: A new
                :class:`singer_sdk.authenticators.APIKeyAuthenticator` instance.
        """
        return cls(stream=stream, key=key, value=value, location=location)


class BearerTokenAuthenticator(APIAuthenticatorBase):
    """Implements bearer token authentication for REST Streams.

    This Authenticator implements Bearer Token authentication. The token
    is a text string, included in the request header and prefixed with
    'Bearer '. The token will be merged with HTTP headers on the stream.
    """

    def __init__(self, stream: RESTStreamBase, token: str) -> None:
        """Create a new authenticator.

        Args:
            stream: The stream instance to use with this authenticator.
            token: Authentication token.
        """
        super().__init__(stream=stream)
        auth_credentials = {"Authorization": f"Bearer {token}"}

        if self._auth_headers is None:
            self._auth_headers = {}
        self._auth_headers.update(auth_credentials)

    @classmethod
    def create_for_stream(
        cls: type[BearerTokenAuthenticator], stream: RESTStreamBase, token: str
    ) -> BearerTokenAuthenticator:
        """Create an Authenticator object specific to the Stream class.

        Args:
            stream: The stream instance to use with this authenticator.
            token: Authentication token.

        Returns:
            BearerTokenAuthenticator: A new
                :class:`singer_sdk.authenticators.BearerTokenAuthenticator` instance.
        """
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
    ) -> None:
        """Create a new authenticator.

        Args:
            stream: The stream instance to use with this authenticator.
            username: API username.
            password: API password.
        """
        super().__init__(stream=stream)
        credentials = f"{username}:{password}".encode()
        auth_token = base64.b64encode(credentials).decode("ascii")
        auth_credentials = {"Authorization": f"Basic {auth_token}"}

        if self._auth_headers is None:
            self._auth_headers = {}
        self._auth_headers.update(auth_credentials)

    @classmethod
    def create_for_stream(
        cls: type[BasicAuthenticator],
        stream: RESTStreamBase,
        username: str,
        password: str,
    ) -> BasicAuthenticator:
        """Create an Authenticator object specific to the Stream class.

        Args:
            stream: The stream instance to use with this authenticator.
            username: API username.
            password: API password.

        Returns:
            BasicAuthenticator: A new
                :class:`singer_sdk.authenticators.BasicAuthenticator` instance.
        """
        return cls(stream=stream, username=username, password=password)


class OAuthAuthenticator(APIAuthenticatorBase):
    """API Authenticator for OAuth 2.0 flows."""

    def __init__(
        self,
        stream: RESTStreamBase,
        auth_endpoint: str | None = None,
        oauth_scopes: str | None = None,
        default_expiration: int | None = None,
    ) -> None:
        """Create a new authenticator.

        Args:
            stream: The stream instance to use with this authenticator.
            auth_endpoint: API username.
            oauth_scopes: API password.
            default_expiration: Default token expiry in seconds.
        """
        super().__init__(stream=stream)
        self._auth_endpoint = auth_endpoint
        self._default_expiration = default_expiration
        self._oauth_scopes = oauth_scopes

        # Initialize internal tracking attributes
        self.access_token: str | None = None
        self.refresh_token: str | None = None
        self.last_refreshed: datetime | None = None
        self.expires_in: int | None = None

    @property
    def auth_headers(self) -> dict:
        """Return a dictionary of auth headers to be applied.

        These will be merged with any `http_headers` specified in the stream.

        Returns:
            HTTP headers for authentication.
        """
        if not self.is_token_valid():
            self.update_access_token()
        result = super().auth_headers
        result["Authorization"] = f"Bearer {self.access_token}"
        return result

    @property
    def auth_endpoint(self) -> str:
        """Get the authorization endpoint.

        Returns:
            The API authorization endpoint if it is set.

        Raises:
            ValueError: If the endpoint is not set.
        """
        if not self._auth_endpoint:
            raise ValueError("Authorization endpoint not set.")
        return self._auth_endpoint

    @property
    def oauth_scopes(self) -> str | None:
        """Get OAuth scopes.

        Returns:
            String of OAuth scopes, or None if not set.
        """
        return self._oauth_scopes

    @property
    def oauth_request_payload(self) -> dict:
        """Get request body.

        Returns:
            A plain (OAuth) or encrypted (JWT) request body.
        """
        return self.oauth_request_body

    @property
    def oauth_request_body(self) -> dict:
        """Get formatted body of the OAuth authorization request.

        Sample implementation:

        .. highlight:: python
        .. code-block:: python

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

        Raises:
            NotImplementedError: If derived class does not override this method.
        """
        raise NotImplementedError(
            "The `oauth_request_body` property was not defined in the subclass."
        )

    @property
    def client_id(self) -> str | None:
        """Get client ID string to be used in authentication.

        Returns:
            Optional client secret from stream config if it has been set.
        """
        if self.config:
            return self.config.get("client_id")
        return None

    @property
    def client_secret(self) -> str | None:
        """Get client secret to be used in authentication.

        Returns:
            Optional client secret from stream config if it has been set.
        """
        if self.config:
            return self.config.get("client_secret")
        return None

    def is_token_valid(self) -> bool:
        """Check if token is valid.

        Returns:
            True if the token is valid (fresh).
        """
        if self.last_refreshed is None:
            return False
        if not self.expires_in:
            return True
        if self.expires_in > (utc_now() - self.last_refreshed).total_seconds():
            return True
        return False

    # Authentication and refresh
    def update_access_token(self) -> None:
        """Update `access_token` along with: `last_refreshed` and `expires_in`.

        Raises:
            RuntimeError: When OAuth login fails.
        """
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
        self.expires_in = token_json.get("expires_in", self._default_expiration)
        if self.expires_in is None:
            self.logger.debug(
                "No expires_in receied in OAuth response and no "
                "default_expiration set. Token will be treated as if it never "
                "expires."
            )
        self.last_refreshed = request_time


class OAuthJWTAuthenticator(OAuthAuthenticator):
    """API Authenticator for OAuth 2.0 flows which utilize a JWT refresh token."""

    @property
    def private_key(self) -> str | None:
        """Return the private key to use in encryption.

        Returns:
            Private key from stream config.
        """
        return self.config.get("private_key", None)

    @property
    def private_key_passphrase(self) -> str | None:
        """Return the private key passphrase to use in encryption.

        Returns:
            Passphrase for private key from stream config.
        """
        return self.config.get("private_key_passphrase", None)

    @property
    def oauth_request_body(self) -> dict:
        """Return request body for OAuth request.

        Returns:
            Request body mapping for OAuth.
        """
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
        """Return request paytload for OAuth request.

        Returns:
            Payload object for OAuth.

        Raises:
            ValueError: If the private key is not set.
        """
        if not self.private_key:
            raise ValueError("Missing 'private_key' property for OAuth payload.")

        private_key: bytes | Any = bytes(self.private_key, "UTF-8")
        if self.private_key_passphrase:
            passphrase = bytes(self.private_key_passphrase, "UTF-8")
            private_key = serialization.load_pem_private_key(
                private_key,
                password=passphrase,
                backend=default_backend(),
            )
        private_key_string: str | Any = private_key.decode("UTF-8")
        return {
            "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
            "assertion": jwt.encode(
                self.oauth_request_body, private_key_string, "RS256"
            ),
        }
