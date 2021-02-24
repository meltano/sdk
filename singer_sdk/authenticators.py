"""Classes to assist in authenticating to APIs."""

import logging
from types import MappingProxyType
import jwt
import math
import requests

from datetime import datetime, timedelta
from typing import Any, Dict, Mapping, Optional

from singer_sdk.helpers.util import utc_now
from singer_sdk.streams import Stream as RESTStreamBase

from singer import utils


class APIAuthenticatorBase(object):
    """Base class for offloading API auth."""

    def __init__(self, stream: RESTStreamBase):
        """Init authenticator."""
        self.tap_name: str = stream.tap_name
        self._config: Dict[str, Any] = dict(stream.config)
        self._http_headers = stream.http_headers
        self.logger: logging.Logger = stream.logger

    @property
    def config(self) -> Mapping[str, Any]:
        """Return a frozen (read-only) config dictionary map."""
        return MappingProxyType(self._config)

    @property
    def http_headers(self) -> dict:
        """Return http headers."""
        return self._http_headers


class SimpleAuthenticator(APIAuthenticatorBase):
    """Base class for offloading API auth."""

    def __init__(self, stream: RESTStreamBase, http_headers: dict = None):
        """Init authenticator.

        If http_headers is provided, it will override the headers specified in `stream`.
        """
        super().__init__(stream=stream)
        if self._http_headers is None:
            self._http_headers = {}
        if http_headers:
            self._http_headers.update(http_headers)


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
    def client_id(self) -> Optional[str]:
        """Return client ID string to be used in authentication or None if not set."""
        if self.config:
            return self.config.get("client_id", self.config.get("client_email"))
        return None

    @property
    def client_secret(self) -> Optional[str]:
        """Return client secret to be used in authentication or None if not set."""
        if self.config:
            return self.config.get("client_secret")
        return None

    @property
    def http_headers(self) -> dict:
        """Return a dictionary of HTTP headers, including any authentication tokens."""
        if not self.is_token_valid():
            self.update_access_token()
        result = super().http_headers
        result["Authorization"] = f"Bearer {self.access_token}"
        return result

    def is_token_valid(self) -> bool:
        """Return true if token is valid."""
        if self.last_refreshed is None:
            return False
        if not self.expires_in:
            return True
        if self.expires_in > (utils.now() - self.last_refreshed).total_seconds():
            return True
        return False

    def update_access_token(self):
        """Update `access_token` along with: `last_refreshed` and `expires_in`."""
        raise NotImplementedError


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

    # Authentication and refresh
    def update_access_token(self):
        """Update `access_token` along with: `last_refreshed` and `expires_in`."""
        request_time = utc_now()
        # jwt_signing_key = jwt.jwk_from_pem(self.private_key)

        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.backends import default_backend

        if self.private_key_passphrase:
            private_key = serialization.load_pem_private_key(
                self.private_key,
                password=self.private_key_passphrase,
                backend=default_backend(),
            )
        else:
            private_key = self.private_key

        auth_request_body = {
            "iss": self.client_id,
            "scope": self.oauth_scopes,
            "aud": self.auth_endpoint,
            "exp": math.floor((request_time + timedelta(hours=1)).timestamp()),
            "iat": math.floor(request_time.timestamp()),
        }
        payload = {
            "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
            "assertion": jwt.encode(auth_request_body, private_key, "RS256"),
        }
        self.logger.info(
            f"Sending JWT token request with body {auth_request_body} "
            f"and payload {payload}"
        )
        token_response = requests.post(self.auth_endpoint, data=payload)
        # self.logger.debug(f"Received JWT request response: {token_response}")
        token_response.raise_for_status()
        token_json = token_response.json()
        self.access_token = token_json["access_token"]
        self.expires_in = token_json["expires_in"]
        # self.logger.debug(f"Received JWT token: {token_json}")
        self.last_refreshed = request_time
