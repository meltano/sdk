import json

import pytest
import requests
from requests.exceptions import HTTPError

from singer_sdk.exceptions import FatalAPIError
from singer_sdk.streams.rest import RESTStream


class CustomResponseValidationStream(RESTStream):
    """Stream with non-conventional error response."""

    url_base = "https://badapi.test"
    name = "imabadapi"
    schema = {"type": "object", "properties": {}}

    def validate_response(self, response: requests.Response):
        super().validate_response(response)
        data = response.json()
        if data["status"] == "ERROR":
            raise FatalAPIError


@pytest.fixture
def basic_rest_stream(rest_tap):
    """Get a RESTful tap stream."""
    return rest_tap.streams["some_stream"]


@pytest.fixture
def custom_validation_stream(rest_tap):
    """Get a RESTful tap stream with custom response validation."""
    return CustomResponseValidationStream(rest_tap)


def test_good_status_code_api(basic_rest_stream):
    fake_response = requests.Response()
    fake_response.status_code = 200
    fake_response.url = basic_rest_stream.url_base

    basic_rest_stream.validate_response(fake_response)


def test_bad_status_code_api(basic_rest_stream):
    fake_response = requests.Response()
    fake_response.status_code = 400
    fake_response.reason = "Bad request :("
    fake_response.url = basic_rest_stream.url_base

    with pytest.raises(FatalAPIError) as exc:
        basic_rest_stream.validate_response(fake_response)

    assert isinstance(exc.value.__cause__, HTTPError)
    assert exc.value.__cause__.response.reason == "Bad request :("


def test_error_status_api(custom_validation_stream):
    fake_response = requests.Response()
    fake_response.status_code = 200
    fake_response._content = json.dumps({"status": "ERROR"}).encode()
    fake_response.url = custom_validation_stream.url_base

    with pytest.raises(FatalAPIError):
        custom_validation_stream.validate_response(fake_response)
