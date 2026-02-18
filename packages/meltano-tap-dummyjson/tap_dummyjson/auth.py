from __future__ import annotations

import logging
import sys
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

import requests.auth
from requests_cache import CachedSession
from singer_sdk.authenticators import SingletonMeta

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    from requests import PreparedRequest, Response

logger = logging.getLogger(__name__)

AUTH_TIMEOUT_SECONDS = 5
EXPIRES_IN_MINS = 30


@dataclass(slots=True, kw_only=True)
class _Token:
    access_token: str
    refresh_token: str
    expires: float

    def is_expired(self) -> bool:
        return self.expires < time.time()


class DummyJSONAuthenticator(requests.auth.AuthBase, metaclass=SingletonMeta):
    """DummyJSON authenticator class."""

    def __init__(
        self,
        *,
        base_url: str,
        username: str,
        password: str,
    ) -> None:
        self.auth_url = f"{base_url}/auth/login"
        self.refresh_token_url = f"{base_url}/refresh"
        self.username = username
        self.password = password

        self._token: _Token | None = None
        self.session = CachedSession(
            ".http_cache",
            backend="filesystem",
            serializer="json",
            allowable_methods=("POST",),
            ignored_parameters=["User-Agent"],
            match_headers=True,
        )

    @override
    def __call__(self, r: PreparedRequest) -> PreparedRequest:
        r.headers["Authorization"] = f"Bearer {self._get_access_token()}"
        return r

    @staticmethod
    def _handle_response(response: Response) -> _Token:
        if response.status_code != 200:
            logger.error("Error: %s", response.text)
            response.raise_for_status()

        data = response.json()
        return _Token(
            access_token=data["accessToken"],
            refresh_token=data["refreshToken"],
            expires=time.time() + EXPIRES_IN_MINS * 60,
        )

    def _get_access_token(self) -> str:
        match self._token:
            case None:  # No token, need to authenticate
                logger.info("No token, authenticating")
                response = self.session.post(
                    self.auth_url,
                    json={
                        "username": self.username,
                        "password": self.password,
                        "expiresInMins": EXPIRES_IN_MINS,
                    },
                    timeout=AUTH_TIMEOUT_SECONDS,
                )
                logger.info("Response status code: %s", response.status_code)
                self._token = self._handle_response(response)
                return self._token.access_token

            case _Token(refresh_token=refresh_token) if self._token.is_expired():
                logger.info("Token expired, refreshing")
                response = self.session.post(
                    self.refresh_token_url,
                    json={
                        "refreshToken": refresh_token,
                        "expiresInMins": EXPIRES_IN_MINS,
                    },
                    timeout=AUTH_TIMEOUT_SECONDS,
                )
                self._token = self._handle_response(response)
                return self._token.access_token

            case _Token(access_token=access_token):  # Token is still valid
                return access_token
