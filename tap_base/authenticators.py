"""Classes to assist in authenticating to APIs."""

import abc
import jwt
import math
import requests

from datetime import datetime, timedelta
from typing import Any, Optional

from tap_base.helpers import utc_now

from singer import utils


class APIAuthenticatorBase(object):
    """Base class for offloading API auth."""

    def __init__(self, config: Optional[dict] = None):
        self._config = config

    @abc.abstractproperty
    def auth_header(self) -> dict:
        pass

    def get_config(self, config_key: str, default: Any = None) -> Any:
        """Return config value or a default value."""
        return (self._config or {}).get(config_key, default)


class SimpleAuthenticator(APIAuthenticatorBase):
    """Base class for offloading API auth."""

    def __init__(self, auth_header: dict = None, config: Optional[dict] = None):
        self._auth_header = auth_header
        super().__init__(config=config)

    @property
    def auth_header(self) -> dict:
        if self._auth_header is None:
            raise NotImplementedError("Auth header has not been initialized.")
        return self._auth_header


class OAuthAuthenticator(APIAuthenticatorBase):
    """API Authenticator for OAuth 2.0 flows which utilize a JWT refresh token."""

    def __init__(
        self,
        config: Optional[dict] = None,
        auth_endpoint: Optional[str] = None,
        oauth_scopes: Optional[str] = None,
    ) -> None:
        super().__init__(config=config)
        # Preserve class-level defaults if they exist:
        if auth_endpoint or not hasattr(self, "auth_endpoint"):
            self.auth_endpoint: Optional[str] = auth_endpoint
        if oauth_scopes or not hasattr(self, "oauth_scopes"):
            self.oauth_scopes: Optional[str] = oauth_scopes

        # Initialize internal auth keys:
        self.client_id: Optional[str] = None
        self.client_secret: Optional[str] = None
        if config:
            # Use client_id, client_email, and/or client_secret by default, if provided.
            self.client_id = config.get("client_id", config.get("client_email", None))
            self.client_secret = config.get("client_secret", None)

        # Initialize internal tracking attributes
        self.access_token: Optional[str] = None
        self.refresh_token: Optional[str] = None
        self.last_refreshed: Optional[datetime] = None
        self.expires_in: Optional[int] = None

    @property
    def auth_header(self) -> dict:
        if not self.is_token_valid():
            self.update_access_token()
        return {"Authorization": f"Bearer {self.access_token}"}

    def is_token_valid(self):
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
    def private_key(self) -> str:
        return self.get_config("private_key")

    # Authentication and refresh
    def update_access_token(self):
        """Update `access_token` along with: `last_refreshed` and `expires_in`."""
        request_time = utc_now()
        jwt_signing_key = jwt.jwk_from_pem(self.private_key)
        auth_request_body = {
            "iss": self.client_id,
            "scope": self.oauth_scopes,
            "aud": self.auth_endpoint,
            "exp": math.floor((request_time + timedelta(hours=1)).timestamp()),
            "iat": math.floor(request_time.timestamp()),
        }
        payload = {
            "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
            "assertion": jwt.JWT().encode(auth_request_body, jwt_signing_key, "RS256"),
        }
        token_response = requests.post(self.auth_endpoint_url, data=payload)
        token_response.raise_for_status()
        token_json = token_response.json()
        self.access_token = token_json["access_token"]
        self.expires_in = token_json["expires_in"]
        self.last_refreshed = request_time
