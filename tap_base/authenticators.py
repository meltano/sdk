"""Classes to assist in authenticating to APIs."""

import abc
from typing import Any, Optional


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


class JWTAuthenticator(APIAuthenticatorBase):
    """API Authenticator for JWT Flows."""

    @property
    def auth_header(self) -> dict:
        if not self.is_token_valid():
            self.access_token = self.get_access_token()
        return {"Authorization": f"Bearer {self.access_token}"}
