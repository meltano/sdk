from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING

import requests
from singer_sdk.authenticators import SingletonMeta

if TYPE_CHECKING:
    from requests import PreparedRequest, Response

logger = logging.getLogger(__name__)

EXPIRES_IN_MINS = 30


class DummyJSONAuthenticator(metaclass=SingletonMeta):
    def __init__(
        self,
        auth_url: str,
        refresh_token_url: str,
        username: str,
        password: str,
    ):
        self.auth_url = auth_url
        self.refresh_token_url = refresh_token_url
        self.username = username
        self.password = password

        self.token: str | None = None
        self.refresh_token: str | None = None

        self.expires: float | None = None
        self.session = requests.Session()

    def __call__(self, request: PreparedRequest) -> PreparedRequest:
        if not self.refresh_token:
            logger.info("Retrieving token")
            self.auth()
            request.headers["Authorization"] = f"Bearer {self.token}"
            return request

        if self.needs_refresh():
            logger.info("Refreshing token")
            self.refresh()
            request.headers["Authorization"] = f"Bearer {self.token}"
            return request

        return request

    def needs_refresh(self) -> bool:
        return self.expires is None or self.expires < time.time()

    def _handle_response(self, response: Response) -> None:
        if response.status_code != 200:
            logger.error("Error: %s", response.text)
            response.raise_for_status()

        data = response.json()
        self.token = data["accessToken"]
        self.refresh_token = data["refreshToken"]
        self.expires = time.time() + EXPIRES_IN_MINS * 60
        logger.info("Authenticated")

    def refresh(self) -> None:
        response = self.session.post(
            self.refresh_token_url,
            json={
                "refreshToken": self.refresh_token,
                "expiresInMins": EXPIRES_IN_MINS,
            },
        )
        self._handle_response(response)

    def auth(self) -> None:
        response = self.session.post(
            self.auth_url,
            json={
                "username": self.username,
                "password": self.password,
                "expiresInMins": EXPIRES_IN_MINS,
            },
        )
        self._handle_response(response)
