import logging
import time

import requests
from singer_sdk.authenticators import SingletonMeta

logger = logging.getLogger(__name__)

EXPIRES_IN_MINS = 30


class DummyJSONAuthenticator(metaclass=SingletonMeta):
    def __init__(self, auth_url, refresh_token_url, username, password):
        self.auth_url = auth_url
        self.refresh_token_url = refresh_token_url
        self.username = username
        self.password = password

        self.token = None
        self.refresh_token = None

        self.expires = 0
        self.session = requests.Session()

    def __call__(self, request):
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
        return True if self.expires is None else self.expires < time.time()

    def _handle_response(self, response):
        if response.status_code != 200:
            logger.error("Error: %s", response.text)
            response.raise_for_status()

        data = response.json()
        self.token = data["accessToken"]
        self.refresh_token = data["refreshToken"]
        self.expires = time.time() + EXPIRES_IN_MINS * 60
        logger.info("Authenticated")

    def refresh(self):
        response = self.session.post(
            self.refresh_token_url,
            json={
                "refreshToken": self.refresh_token,
                "expiresInMins": EXPIRES_IN_MINS,
            },
        )
        self._handle_response(response)

    def auth(self):
        response = self.session.post(
            self.auth_url,
            json={
                "username": self.username,
                "password": self.password,
                "expiresInMins": EXPIRES_IN_MINS,
            },
        )
        self._handle_response(response)
