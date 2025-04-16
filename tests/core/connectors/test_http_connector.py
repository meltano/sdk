from __future__ import annotations

import json
import typing as t

import requests
import werkzeug
from requests.adapters import BaseAdapter

from singer_sdk.authenticators import HeaderAuth
from singer_sdk.connectors import HTTPConnector

if t.TYPE_CHECKING:
    from pytest_httpserver import HTTPServer


class MockAdapter(BaseAdapter):
    def send(
        self,
        request: requests.PreparedRequest,
        stream: bool = False,  # noqa: FBT002
        timeout: float | tuple[float, float] | tuple[float, None] | None = None,
        verify: bool | str = True,  # noqa: FBT002
        cert: bytes | str | tuple[bytes | str, bytes | str] | None = None,
        proxies: t.Mapping[str, str] | None = None,
    ) -> requests.Response:
        """Send a request."""
        response = requests.Response()
        data = {
            "url": request.url,
            "headers": dict(request.headers),
            "method": request.method,
            "body": request.body,
            "stream": stream,
            "timeout": timeout,
            "verify": verify,
            "cert": cert,
            "proxies": proxies,
        }
        response.status_code = 200
        response._content = json.dumps(data).encode("utf-8")
        return response

    def close(self) -> None:
        pass


class HeaderAuthConnector(HTTPConnector):
    def get_authenticator(self) -> HeaderAuth:
        return HeaderAuth("Bearer", self.config["token"])


def test_base_connector(httpserver: HTTPServer):
    connector = HTTPConnector({})

    httpserver.expect_request("").respond_with_json({"foo": "bar"})
    url = httpserver.url_for("/")

    response = connector.request("GET", url)
    data = response.json()
    assert data["foo"] == "bar"


def test_auth(httpserver: HTTPServer):
    connector = HeaderAuthConnector({"token": "s3cr3t"})

    def _handler(request: werkzeug.Request) -> werkzeug.Response:
        return werkzeug.Response(
            json.dumps(
                {
                    "headers": dict(request.headers),
                    "url": request.url,
                },
            ),
            status=200,
            mimetype="application/json",
        )

    httpserver.expect_request("").respond_with_handler(_handler)
    url = httpserver.url_for("/")

    response = connector.request("GET", url)
    data = response.json()
    assert data["headers"]["Authorization"] == "Bearer s3cr3t"

    response = connector.request("GET", url, authenticate=False)
    data = response.json()
    assert "Authorization" not in data["headers"]


def test_custom_adapters():
    class MyConnector(HTTPConnector):
        @property
        def adapters(self) -> dict[str, BaseAdapter]:
            return {
                "https://test": MockAdapter(),
            }

    connector = MyConnector({})
    response = connector.request("GET", "https://test")
    data = response.json()

    assert data["url"] == "https://test/"
    assert data["headers"]
    assert data["method"] == "GET"
