"""Stream tests."""

from __future__ import annotations

import datetime
import decimal
import json
import logging
import typing as t
import urllib.parse
import warnings

import pytest
import requests
import requests_mock.adapter as requests_mock_adapter

from singer_sdk._singerlib import Catalog, MetadataMapping
from singer_sdk.exceptions import (
    FatalAPIError,
    InvalidReplicationKeyException,
    RetriableAPIError,
)
from singer_sdk.helpers._classproperty import classproperty
from singer_sdk.helpers._compat import SingerSDKDeprecationWarning
from singer_sdk.helpers._compat import datetime_fromisoformat as parse
from singer_sdk.helpers.jsonpath import _compile_jsonpath
from singer_sdk.streams.core import REPLICATION_FULL_TABLE, REPLICATION_INCREMENTAL
from singer_sdk.streams.graphql import GraphQLStream
from singer_sdk.streams.jsonrpc import JSONRPCStream
from singer_sdk.streams.rest import RESTStream
from singer_sdk.typing import IntegerType, PropertiesList, Property, StringType
from tests.core.conftest import SimpleTestStream

if t.TYPE_CHECKING:
    import requests_mock

    from singer_sdk import Stream, Tap
    from tests.core.conftest import SimpleTestTap

CONFIG_START_DATE = "2021-01-01"


class RestTestStream(RESTStream):
    """Test RESTful stream class."""

    name = "restful"
    path = "/example"
    url_base = "https://example.com"
    schema = PropertiesList(
        Property("id", IntegerType, required=True),
        Property("value", StringType, required=True),
    ).to_dict()
    replication_key = "updatedAt"


class RestTestStreamLegacyPagination(RestTestStream):
    """Test RESTful stream class with pagination."""

    def get_next_page_token(
        self,
        response: requests.Response,  # noqa: ARG002
        previous_token: int | None,
    ) -> int:
        return previous_token + 1 if previous_token is not None else 1


class GraphqlTestStream(GraphQLStream):
    """Test Graphql stream class."""

    name = "graphql"
    path = "/example"
    url_base = "https://example.com"
    schema = PropertiesList(
        Property("id", IntegerType, required=True),
        Property("value", StringType, required=True),
    ).to_dict()
    replication_key = "updatedAt"


class JsonRpcTestStream(JSONRPCStream):
    """Test JSON-RPC stream class."""

    name = "jsonrpc"
    path = "/jsonrpc"
    url_base = "https://example.com"
    schema = PropertiesList(
        Property("id", IntegerType, required=True),
        Property("name", StringType, required=True),
    ).to_dict()
    method = "get_items"

    def get_new_paginator(self):
        """Return a fresh paginator for this endpoint."""
        return self.NoNextPageTokenPaginator()

    class NoNextPageTokenPaginator:
        """A paginator that doesn't provide a next page token."""

        def __init__(self) -> None:
            self.current_value = None
            self.finished = False

        def get_next(self, response=None):  # noqa: ARG002
            """Return next page token, which is None."""
            if self.finished:
                return
            return

        def advance(self, response=None):  # noqa: ARG002
            """Mark as finished after first page."""
            self.finished = True


@pytest.fixture
def stream(tap):
    """Create a new stream instance."""
    return tap.load_streams()[0]


def test_stream_apply_catalog(stream: Stream):
    """Applying a catalog to a stream should overwrite fields."""
    assert stream.primary_keys == []
    assert stream.replication_key == "updatedAt"
    assert stream.replication_method == REPLICATION_INCREMENTAL
    assert stream.forced_replication_method is None

    stream.apply_catalog(
        catalog=Catalog.from_dict(
            {
                "streams": [
                    {
                        "tap_stream_id": stream.name,
                        "metadata": MetadataMapping(),
                        "key_properties": ["id"],
                        "stream": stream.name,
                        "schema": stream.schema,
                        "replication_method": REPLICATION_FULL_TABLE,
                        "replication_key": None,
                    },
                ],
            },
        ),
    )

    assert stream.primary_keys == ["id"]
    assert stream.replication_key is None
    assert stream.replication_method == REPLICATION_FULL_TABLE
    assert stream.forced_replication_method == REPLICATION_FULL_TABLE


def test_stream_apply_catalog__singer_standard(stream: Stream):
    """Applying a catalog to a stream should overwrite fields."""
    assert stream.primary_keys == []
    assert stream.replication_key == "updatedAt"
    assert stream.replication_method == REPLICATION_INCREMENTAL
    assert stream.forced_replication_method is None

    stream.apply_catalog(
        catalog=Catalog.from_dict(
            {
                "streams": [
                    {
                        "tap_stream_id": stream.name,
                        "stream": stream.name,
                        "schema": stream.schema,
                        "metadata": [
                            {
                                "breadcrumb": [],
                                "metadata": {
                                    "table-key-properties": ["id"],
                                    "replication-key": "newReplicationKey",
                                    "forced-replication-method": REPLICATION_FULL_TABLE,
                                },
                            },
                        ],
                    },
                ],
            },
        ),
    )

    assert stream.primary_keys == ["id"]
    assert stream.replication_key == "newReplicationKey"
    assert stream.replication_method == REPLICATION_FULL_TABLE
    assert stream.forced_replication_method == REPLICATION_FULL_TABLE


@pytest.mark.parametrize(
    "stream_name,forced_replication_method,bookmark_value,expected_starting_value",
    [
        pytest.param(
            "test",
            None,
            None,
            parse(CONFIG_START_DATE).replace(tzinfo=datetime.timezone.utc),
            id="datetime-repl-key-no-state",
        ),
        pytest.param(
            "test",
            None,
            "2021-02-01",
            datetime.datetime(2021, 2, 1, tzinfo=datetime.timezone.utc),
            id="datetime-repl-key-recent-bookmark",
        ),
        pytest.param(
            "test",
            REPLICATION_FULL_TABLE,
            "2021-02-01",
            None,
            id="datetime-forced-full-table",
        ),
        pytest.param(
            "test",
            None,
            "2020-01-01",
            parse(CONFIG_START_DATE).replace(tzinfo=datetime.timezone.utc),
            id="datetime-repl-key-old-bookmark",
        ),
        pytest.param(
            "test",
            None,
            "2021-01-02T00:00:00-08:00",
            datetime.datetime(
                2021,
                1,
                2,
                tzinfo=datetime.timezone(datetime.timedelta(hours=-8)),
            ),
            id="datetime-repl-key-recent-bookmark-tz-aware",
        ),
        pytest.param(
            "unix_ts",
            None,
            None,
            CONFIG_START_DATE,
            id="naive-unix-ts-repl-key-no-state",
        ),
        pytest.param(
            "unix_ts",
            None,
            "1612137600",
            "1612137600",
            id="naive-unix-ts-repl-key-recent-bookmark",
        ),
        pytest.param(
            "unix_ts",
            None,
            "1577858400",
            "1577858400",
            id="naive-unix-ts-repl-key-old-bookmark",
        ),
        pytest.param(
            "unix_ts_override",
            None,
            None,
            CONFIG_START_DATE,
            id="unix-ts-repl-key-no-state",
        ),
        pytest.param(
            "unix_ts_override",
            None,
            "1612137600",
            "1612137600",
            id="unix-ts-repl-key-recent-bookmark",
        ),
        pytest.param(
            "unix_ts_override",
            None,
            "1577858400",
            parse(CONFIG_START_DATE).timestamp(),
            id="unix-ts-repl-key-old-bookmark",
        ),
    ],
)
def test_stream_starting_timestamp(
    tap: Tap,
    stream_name: str,
    forced_replication_method: str | None,
    bookmark_value: str,
    expected_starting_value: t.Any,
):
    """Test the starting timestamp for a stream."""
    stream = tap.streams[stream_name]

    if stream.is_timestamp_replication_key:
        get_starting_value = stream.get_starting_timestamp
    else:
        get_starting_value = stream.get_starting_replication_key_value

    tap.load_state(
        {
            "bookmarks": {
                stream_name: {
                    "replication_key": stream.replication_key,
                    "replication_key_value": bookmark_value,
                },
            },
        },
    )
    stream._write_starting_replication_value(None)

    with stream.with_replication_method(forced_replication_method):
        assert get_starting_value(None) == expected_starting_value


def test_stream_invalid_replication_key(tap: SimpleTestTap):
    """Validate an exception is raised if replication_key not in schema."""

    class InvalidReplicationKeyStream(SimpleTestStream):
        replication_key = "INVALID"

    stream = InvalidReplicationKeyStream(tap)

    with pytest.raises(
        InvalidReplicationKeyException,
        match=(
            f"Field '{stream.replication_key}' is not in schema for stream "
            f"'{stream.name}'"
        ),
    ):
        _check = stream.is_timestamp_replication_key


@pytest.mark.parametrize(
    "path,content,result",
    [
        (
            "$[*]",
            '[{"id": 1, "value": "abc"}, {"id": 2, "value": "def"}]',
            [{"id": 1, "value": "abc"}, {"id": 2, "value": "def"}],
        ),
        (
            "$.data[*]",
            '{"data": [{"id": 1, "value": "abc"}, {"id": 2, "value": "def"}]}',
            [{"id": 1, "value": "abc"}, {"id": 2, "value": "def"}],
        ),
        (
            "$.data.records[*]",
            """{
                "data": {
                    "records": [
                        {"id": 1, "value": "abc"},
                        {"id": 2, "value": "def"}
                    ]
                }
            }""",
            [{"id": 1, "value": "abc"}, {"id": 2, "value": "def"}],
        ),
        (
            "$",
            '{"id": 1, "value": "abc"}',
            [{"id": 1, "value": "abc"}],
        ),
        (
            "$.data.*",
            """
            {
              "data": {
                "1": {
                  "id": 1,
                  "value": "abc"
                },
                "2": {
                  "id": 2,
                  "value": "def"
                }
              }
            }
            """,
            [{"id": 1, "value": "abc"}, {"id": 2, "value": "def"}],
        ),
    ],
    ids=[
        "array",
        "nested_one_level",
        "nested_two_levels",
        "single_object",
        "nested_values",
    ],
)
def test_jsonpath_rest_stream(tap: Tap, path: str, content: str, result: list[dict]):
    """Validate records are extracted correctly from the API response."""
    fake_response = requests.Response()
    fake_response._content = str.encode(content)

    RestTestStream.records_jsonpath = path
    stream = RestTestStream(tap)

    records = stream.parse_response(fake_response)

    assert list(records) == result


def test_legacy_pagination(tap: Tap):
    """Validate legacy pagination is handled correctly."""
    stream = RestTestStreamLegacyPagination(tap)

    with pytest.deprecated_call():
        stream.get_new_paginator()

    page: int | None = None
    page = stream.get_next_page_token(None, page)
    assert page == 1

    page = stream.get_next_page_token(None, page)
    assert page == 2


def test_jsonpath_graphql_stream_default(tap: Tap):
    """Validate graphql JSONPath, defaults to the stream name."""
    content = """{
                "data": {
                    "graphql": [
                        {"id": 1, "value": "abc"},
                        {"id": 2, "value": "def"}
                    ]
                }
            }"""

    fake_response = requests.Response()
    fake_response._content = str.encode(content)

    stream = GraphqlTestStream(tap)
    records = stream.parse_response(fake_response)

    assert list(records) == [{"id": 1, "value": "abc"}, {"id": 2, "value": "def"}]


def test_jsonpath_graphql_stream_override(tap: Tap):
    """Validate graphql jsonpath can be updated."""
    content = """[
                        {"id": 1, "value": "abc"},
                        {"id": 2, "value": "def"}
                    ]
            """

    fake_response = requests.Response()
    fake_response._content = str.encode(content)

    class GraphQLJSONPathOverride(GraphqlTestStream):
        @classproperty
        def records_jsonpath(cls):  # noqa: N805
            return "$[*]"

    stream = GraphQLJSONPathOverride(tap)

    records = stream.parse_response(fake_response)

    assert list(records) == [{"id": 1, "value": "abc"}, {"id": 2, "value": "def"}]


@pytest.mark.parametrize(
    "path,content,headers,result",
    [
        (
            "$.next_page",
            '{"data": [], "next_page": "xyz123"}',
            {},
            "xyz123",
        ),
        (
            "$.next_page",
            '{"data": [], "next_page": null}',
            {},
            None,
        ),
        (
            "$.next_page",
            '{"data": []}',
            {},
            None,
        ),
        (
            None,
            '[{"id": 1, "value": "abc"}',
            {"X-Next-Page": "xyz123"},
            "xyz123",
        ),
        (
            "$.link[?(@.relation=='next')].url",
            """
            {
              "link": [
                {
                  "relation": "previous",
                  "url": "https://myapi.test/6"
                },
                {
                  "relation": "next",
                  "url": "https://myapi.test/8"
                },
                {
                  "relation": "first",
                  "url": "https://myapi.test/1"
                },
                {
                  "relation": "last",
                  "url": "https://myapi.test/20"
                }
              ]
            }
            """,
            {},
            "https://myapi.test/8",
        ),
    ],
    ids=[
        "has_next_page",
        "null_next_page",
        "no_next_page_key",
        "use_header",
        "filtered_hateoas",
    ],
)
def test_next_page_token_jsonpath(
    tap: Tap,
    path: str,
    content: str,
    headers: dict,
    result: str,
):
    """Validate pagination token is extracted correctly from API response."""
    fake_response = requests.Response()
    fake_response.headers.update(headers)
    fake_response._content = str.encode(content)

    RestTestStream.next_page_token_jsonpath = path
    stream = RestTestStream(tap)

    paginator = stream.get_new_paginator()
    next_page = paginator.get_next(fake_response)
    assert next_page == result


def test_cached_jsonpath():
    """Test compiled JSONPath is cached."""
    expression = "$[*]"
    compiled = _compile_jsonpath(expression)
    recompiled = _compile_jsonpath(expression)

    # cached objects should point to the same memory location
    assert recompiled is compiled


def test_sync_costs_calculation(tap: Tap, caplog):
    """Test sync costs are added up correctly."""
    fake_request = requests.PreparedRequest()
    fake_response = requests.Response()

    stream = RestTestStream(tap)

    def calculate_test_cost(
        request: requests.PreparedRequest,  # noqa: ARG001
        response: requests.Response,  # noqa: ARG001
        context: dict | None,  # noqa: ARG001
    ):
        return {"dim1": 1, "dim2": 2}

    stream.calculate_sync_cost = calculate_test_cost
    stream.update_sync_costs(fake_request, fake_response, None)
    stream.update_sync_costs(fake_request, fake_response, None)
    assert stream._sync_costs == {"dim1": 2, "dim2": 4}

    with caplog.at_level(logging.INFO, logger=tap.name):
        stream.log_sync_costs()

    assert len(caplog.records) == 1

    for record in caplog.records:
        assert record.levelname == "INFO"
        assert f"Total Sync costs for stream {stream.name}" in record.message


def test_non_json_payload(tap: Tap, requests_mock: requests_mock.Mocker):
    """Test non-JSON payload is handled correctly."""

    def callback(request: requests.PreparedRequest, context: requests_mock.Context):  # noqa: ARG001
        assert request.headers["Content-Type"] == "application/x-www-form-urlencoded"
        assert request.body == "my_key=my_value"

        data = urllib.parse.parse_qs(request.body)

        return {
            "data": [
                {"id": 1, "value": f"{data['my_key'][0]}_1"},
                {"id": 2, "value": f"{data['my_key'][0]}_2"},
            ]
        }

    class NonJsonStream(RestTestStream):
        payload_as_json = False
        http_method = "POST"
        path = "/non-json"
        records_jsonpath = "$.data[*]"

        def prepare_request_payload(self, context, next_page_token):  # noqa: ARG002
            return {"my_key": "my_value"}

    stream = NonJsonStream(tap)

    requests_mock.post(
        "https://example.com/non-json",
        json=callback,
    )

    records = list(stream.request_records(None))
    assert records == [
        {"id": 1, "value": "my_value_1"},
        {"id": 2, "value": "my_value_2"},
    ]


def test_mutate_http_method(tap: Tap, requests_mock: requests_mock.Mocker):
    """Test HTTP method can be overridden."""

    def callback(request: requests.PreparedRequest, context: requests_mock.Context):
        if request.method == "POST":
            return {
                "data": [
                    {"id": 1, "value": "abc"},
                    {"id": 2, "value": "def"},
                ]
            }

        # Method not allowed
        context.status_code = 405
        context.reason = "Method Not Allowed"
        return {"error": "Check your method"}

    with warnings.catch_warnings():
        warnings.simplefilter("error")
        assert RestTestStream(tap).http_method == "GET"

    class PostStream(RestTestStream):
        records_jsonpath = "$.data[*]"
        path = "/endpoint"

    stream = PostStream(tap, http_method="PUT")
    requests_mock.request(
        requests_mock_adapter.ANY,
        url="https://example.com/endpoint",
        json=callback,
    )

    with pytest.raises(FatalAPIError, match="Method Not Allowed"):
        list(stream.request_records(None))

    assert hasattr(stream, "http_method")
    assert not hasattr(stream, "rest_method")

    stream.http_method = None
    stream.rest_method = "GET"

    with (
        pytest.raises(FatalAPIError, match="Method Not Allowed"),
        pytest.warns(SingerSDKDeprecationWarning),
    ):
        list(stream.request_records(None))

    stream.http_method = "POST"

    records = list(stream.request_records(None))
    assert records == [
        {"id": 1, "value": "abc"},
        {"id": 2, "value": "def"},
    ]


# JSON-RPC Stream Tests


def test_jsonrpc_request_payload(tap: Tap):
    """Test that JSON-RPC request payloads are correctly formatted."""
    stream = JsonRpcTestStream(tap)

    # Prepare a request for testing
    request = stream.prepare_request(None, None)

    # Test request method
    assert request.method == "POST"

    # Test content type
    assert "application/json" in request.headers.get("Content-Type", "")

    # Parse the request body
    payload = json.loads(request.body)

    # Verify JSON-RPC structure
    assert payload["jsonrpc"] == "2.0"
    assert payload["method"] == "get_items"
    assert "id" in payload

    # Params should not be present if empty
    assert "params" not in payload


def test_jsonrpc_with_params(tap: Tap):
    """Test JSON-RPC request with parameters."""
    stream = JsonRpcTestStream(tap)

    # Override get_method_parameters to return test params
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(
            stream,
            "get_method_parameters",
            lambda ctx=None, next_page_token=None: {"page": 1, "limit": 10},  # noqa: ARG005
        )
        request = stream.prepare_request(None, None)

    payload = json.loads(request.body)

    # Verify params are included
    assert "params" in payload
    assert payload["params"] == {"page": 1, "limit": 10}


def test_jsonrpc_parse_response(tap: Tap):
    """Test parsing various JSON-RPC response formats."""
    stream = JsonRpcTestStream(tap)

    # Test list result
    response = requests.Response()
    response._content = json.dumps(
        {
            "jsonrpc": "2.0",
            "id": "123",
            "result": [{"id": 1, "name": "Item 1"}, {"id": 2, "name": "Item 2"}],
        }
    ).encode()

    records = list(stream.parse_response(response))
    assert len(records) == 2
    assert records[0]["id"] == 1
    assert records[0]["name"] == "Item 1"
    assert records[1]["id"] == 2
    assert records[1]["name"] == "Item 2"

    # Test dict result with data key
    response._content = json.dumps(
        {
            "jsonrpc": "2.0",
            "id": "123",
            "result": {
                "data": [{"id": 3, "name": "Item 3"}, {"id": 4, "name": "Item 4"}]
            },
        }
    ).encode()

    records = list(stream.parse_response(response))
    assert len(records) == 2
    assert records[0]["id"] == 3
    assert records[0]["name"] == "Item 3"
    assert records[1]["id"] == 4
    assert records[1]["name"] == "Item 4"

    # Test dict result without data key
    response._content = json.dumps(
        {
            "jsonrpc": "2.0",
            "id": "123",
            "result": {"id": 5, "name": "Item 5"},
        }
    ).encode()

    records = list(stream.parse_response(response))
    assert len(records) == 1
    assert records[0]["id"] == 5
    assert records[0]["name"] == "Item 5"


def test_jsonrpc_error_handling(tap: Tap, requests_mock: requests_mock.Mocker):
    """Test JSON-RPC error handling."""
    stream = JsonRpcTestStream(tap)

    # Mock client error with proper Content-Type header
    requests_mock.post(
        "https://example.com/jsonrpc",
        json={
            "jsonrpc": "2.0",
            "id": "123",
            "error": {
                "code": -32602,
                "message": "Invalid params",
                "data": {"details": "missing required field"},
            },
        },
        headers={"Content-Type": "application/json"},
    )

    # Should raise FatalAPIError for client errors (like Invalid params)
    with pytest.raises(FatalAPIError) as excinfo:
        list(stream.request_records(None))

    assert "Invalid params" in str(excinfo.value)

    # Mock server error with proper Content-Type header
    # Server errors in JSON-RPC are in the -32000 to -32099 range
    requests_mock.post(
        "https://example.com/jsonrpc",
        json={
            "jsonrpc": "2.0",
            "id": "123",
            "error": {
                "code": -32050,  # A server error code in the JSON-RPC spec
                "message": "Server error",
            },
        },
        headers={"Content-Type": "application/json"},
    )

    # Should raise RetriableAPIError for server errors in the allowed range
    with pytest.raises(RetriableAPIError) as excinfo:
        list(stream.request_records(None))

    assert "Server error" in str(excinfo.value)


def test_jsonrpc_successful_request(tap: Tap, requests_mock: requests_mock.Mocker):
    """Test complete JSON-RPC request/response cycle."""
    stream = JsonRpcTestStream(tap)

    # Mock successful response with proper Content-Type header
    requests_mock.post(
        "https://example.com/jsonrpc",
        json={
            "jsonrpc": "2.0",
            "id": "123",
            "result": [{"id": 1, "name": "Item 1"}, {"id": 2, "name": "Item 2"}],
        },
        headers={"Content-Type": "application/json"},
    )

    # Test full request/response cycle
    records = list(stream.request_records(None))

    # Verify records
    assert len(records) == 2
    assert records[0]["id"] == 1
    assert records[0]["name"] == "Item 1"
    assert records[1]["id"] == 2
    assert records[1]["name"] == "Item 2"

    # Verify request was made with correct format
    request = requests_mock.last_request
    payload = json.loads(request.body)
    assert payload["jsonrpc"] == "2.0"
    assert payload["method"] == "get_items"
    assert "id" in payload


def test_parse_response(tap: Tap):
    content = """[
        {"id": 1, "value": 3.14159},
        {"id": 2, "value": 2.71828}
    ]
    """

    class MyRESTStream(RESTStream):
        url_base = "https://example.com"
        path = "/dummy"
        name = "dummy"
        schema = {  # noqa: RUF012
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "value": {"type": "number"},
            },
        }

    stream = MyRESTStream(tap=tap)

    response = requests.Response()
    response._content = content.encode("utf-8")

    records = list(stream.parse_response(response))
    assert records == [
        {"id": 1, "value": decimal.Decimal("3.14159")},
        {"id": 2, "value": decimal.Decimal("2.71828")},
    ]


@pytest.mark.parametrize(
    "input_catalog,selection",
    [
        pytest.param(
            None,
            {
                "selected_stream": True,
                "unselected_stream": False,
            },
            id="no_catalog",
        ),
        pytest.param(
            {
                "streams": [],
            },
            {
                "selected_stream": False,
                "unselected_stream": False,
            },
            id="empty_catalog",
        ),
        pytest.param(
            {
                "streams": [
                    {
                        "tap_stream_id": "selected_stream",
                        "metadata": [
                            {
                                "breadcrumb": [],
                                "metadata": {
                                    "selected": True,
                                },
                            },
                        ],
                    },
                    {
                        "tap_stream_id": "unselected_stream",
                        "metadata": [
                            {
                                "breadcrumb": [],
                                "metadata": {
                                    "selected": True,
                                },
                            },
                        ],
                    },
                ],
            },
            {
                "selected_stream": True,
                "unselected_stream": True,
            },
            id="catalog_with_selection",
        ),
    ],
)
def test_stream_class_selection(tap_class, input_catalog, selection):
    """Test stream class selection."""

    class SelectedStream(RESTStream):
        name = "selected_stream"
        url_base = "https://example.com"
        schema = {"type": "object", "properties": {}}  # noqa: RUF012

    class UnselectedStream(SelectedStream):
        name = "unselected_stream"
        selected_by_default = False

    class MyTap(tap_class):
        def discover_streams(self):
            return [SelectedStream(self), UnselectedStream(self)]

    # Check that the selected stream is selected
    tap = MyTap(config=None, catalog=input_catalog, validate_config=False)
    assert all(
        tap.streams[stream].selected is selection[stream] for stream in selection
    )


def test_jsonrpc_custom_version(tap: Tap):
    """Test custom JSON-RPC version."""

    class CustomVersionJsonRpcStream(JsonRpcTestStream):
        jsonrpc_version = "1.0"  # Use an older version

    stream = CustomVersionJsonRpcStream(tap)
    request = stream.prepare_request(None, None)
    payload = json.loads(request.body)

    # Verify custom version is used
    assert payload["jsonrpc"] == "1.0"


def test_jsonrpc_batch_response(tap: Tap):
    """Test handling batch responses from JSON-RPC endpoint."""
    stream = JsonRpcTestStream(tap)

    # Test batch result handling - multiple results in a list
    response = requests.Response()
    response._content = json.dumps(
        {
            "jsonrpc": "2.0",
            "id": "123",
            "result": {
                "items": [{"id": 1, "name": "Item 1"}, {"id": 2, "name": "Item 2"}],
                "total": 2,
                "page": 1,
            },
        }
    ).encode()

    records = list(stream.parse_response(response))
    assert len(records) == 2
    assert records[0]["id"] == 1
    assert records[1]["id"] == 2


def test_jsonrpc_pagination(tap: Tap, requests_mock: requests_mock.Mocker):
    """Test JSON-RPC pagination using params."""

    class PaginatedJsonRpcStream(JSONRPCStream):
        """Test JSON-RPC stream with pagination."""

        name = "paginated_jsonrpc"
        path = "/jsonrpc"
        url_base = "https://example.com"
        schema = PropertiesList(
            Property("id", IntegerType, required=True),
            Property("name", StringType, required=True),
        ).to_dict()
        method = "get_items"

        def get_new_paginator(self):
            """Return a paginator for this endpoint."""
            return self.JsonRpcPaginator(start_page=1)

        class JsonRpcPaginator:
            """A paginator that uses page numbers."""

            def __init__(self, start_page):
                self.current_page = start_page
                self.current_value = {"page": start_page}  # Add current_value property
                self.finished = False
                self.total_pages = None

            def get_next(self, response):
                """Get next page token."""
                if self.finished:
                    return None

                response_json = response.json()
                if "result" in response_json and isinstance(
                    response_json["result"], dict
                ):
                    result = response_json["result"]
                    self.total_pages = result.get("total_pages", 1)

                    if self.current_page >= self.total_pages:
                        self.finished = True
                        return None

                    self.current_page += 1
                    next_value = {"page": self.current_page}
                    self.current_value = next_value  # Update current_value
                    return next_value
                return None

            def advance(self, response):
                """Advance the paginator using the response."""
                self.get_next(response)

        def get_method_parameters(self, context=None, next_page_token=None):  # noqa: ARG002
            """Add pagination parameters."""
            params = {}
            if next_page_token:
                params.update(next_page_token)
            else:
                params["page"] = 1  # Start with page 1

            return params

    stream = PaginatedJsonRpcStream(tap)

    # Mock first page response
    requests_mock.post(
        "https://example.com/jsonrpc",
        [
            {
                "json": {
                    "jsonrpc": "2.0",
                    "id": "123",
                    "result": {
                        "items": [
                            {"id": 1, "name": "Item 1"},
                            {"id": 2, "name": "Item 2"},
                        ],
                        "total_pages": 2,
                        "page": 1,
                    },
                },
                "headers": {"Content-Type": "application/json"},
            },
            {
                "json": {
                    "jsonrpc": "2.0",
                    "id": "456",
                    "result": {
                        "items": [
                            {"id": 3, "name": "Item 3"},
                            {"id": 4, "name": "Item 4"},
                        ],
                        "total_pages": 2,
                        "page": 2,
                    },
                },
                "headers": {"Content-Type": "application/json"},
            },
        ],
    )

    # Request all records across pages
    records = list(stream.request_records(None))

    # Verify we got all records from both pages
    assert len(records) == 4
    assert [r["id"] for r in records] == [1, 2, 3, 4]

    # Verify the correct pagination parameters were sent
    request_bodies = [json.loads(r.body) for r in requests_mock.request_history]
    assert request_bodies[0]["params"]["page"] == 1
    assert request_bodies[1]["params"]["page"] == 2


def test_jsonrpc_complex_parameters(tap: Tap, requests_mock: requests_mock.Mocker):
    """Test JSON-RPC request with complex nested parameters."""

    class ComplexParamsJsonRpcStream(JsonRpcTestStream):
        def get_method_parameters(
            self,
            context=None,  # noqa: ARG002
            next_page_token=None,  # noqa: ARG002
        ):  # Add default value to make it optional
            """Return complex nested parameters."""
            # context and next_page_token are not used in this implementation
            return {
                "filter": {
                    "active": True,
                    "date_range": {"start": "2021-01-01", "end": "2021-12-31"},
                },
                "options": {
                    "limit": 100,
                    "sort": [{"field": "created_at", "order": "desc"}],
                },
            }

    stream = ComplexParamsJsonRpcStream(tap)

    # Mock successful response
    requests_mock.post(
        "https://example.com/jsonrpc",
        json={"jsonrpc": "2.0", "id": "123", "result": [{"id": 1, "name": "Item 1"}]},
        headers={"Content-Type": "application/json"},
    )

    # Make the request
    list(stream.request_records(None))

    # Verify complex parameters were sent correctly
    request = requests_mock.last_request
    payload = json.loads(request.body)

    assert "params" in payload
    assert payload["params"]["filter"]["active"] is True
    assert payload["params"]["filter"]["date_range"]["start"] == "2021-01-01"
    assert payload["params"]["options"]["sort"][0]["field"] == "created_at"


def test_jsonrpc_empty_result(tap: Tap):
    """Test handling empty results from JSON-RPC endpoint."""
    stream = JsonRpcTestStream(tap)

    # Test empty list result
    response = requests.Response()
    response._content = json.dumps(
        {"jsonrpc": "2.0", "id": "123", "result": []}
    ).encode()

    records = list(stream.parse_response(response))
    assert len(records) == 0

    # Test empty object result
    response._content = json.dumps(
        {"jsonrpc": "2.0", "id": "123", "result": {}}
    ).encode()

    records = list(stream.parse_response(response))
    assert len(records) == 1  # Empty dict treated as a single record
    assert records[0] == {}

    # Test null result
    response._content = json.dumps(
        {"jsonrpc": "2.0", "id": "123", "result": None}
    ).encode()

    records = list(stream.parse_response(response))
    assert len(records) == 1
    assert records[0] == {"result": None}


def test_jsonrpc_no_result(tap: Tap):
    """Test handling response with missing result field."""
    stream = JsonRpcTestStream(tap)

    response = requests.Response()
    response._content = json.dumps(
        {
            "jsonrpc": "2.0",
            "id": "123",
            # No result field
        }
    ).encode()

    records = list(stream.parse_response(response))
    assert len(records) == 0  # Should return empty list when no result field is present


def test_jsonrpc_notification(tap: Tap):
    """Test JSON-RPC request with no ID (notification)."""

    class JsonRpcNotificationStream(JsonRpcTestStream):
        def prepare_request_payload(self, context, next_page_token):
            """Return a notification payload (no ID)."""
            params = self.get_method_parameters(context, next_page_token)
            payload = {"jsonrpc": self.jsonrpc_version, "method": self.method}

            if params:
                payload["params"] = params

            return payload

    stream = JsonRpcNotificationStream(tap)
    request = stream.prepare_request(None, None)
    payload = json.loads(request.body)

    # Verify ID is not present
    assert "id" not in payload
    assert payload["jsonrpc"] == "2.0"
    assert payload["method"] == "get_items"


def test_jsonrpc_records_jsonpath(tap: Tap):
    """Test that the JSONRPCStream records_jsonpath works correctly."""
    stream = JsonRpcTestStream(tap)

    # Verify that the default records_jsonpath is set correctly
    assert stream.records_jsonpath == "$.result[*]"

    # Create a response with data that matches the default JSONPath
    response = requests.Response()
    response._content = json.dumps(
        {
            "jsonrpc": "2.0",
            "id": "123",
            "result": [{"id": 1, "name": "Record 1"}, {"id": 2, "name": "Record 2"}],
        }
    ).encode()

    # Parse using the default implementation, which should use records_jsonpath
    records = list(stream.parse_response(response))
    assert len(records) == 2
    assert records[0]["id"] == 1
    assert records[1]["id"] == 2

    # Now test with a custom records_jsonpath
    class CustomJsonPathJsonRpcStream(JsonRpcTestStream):
        @classproperty
        def records_jsonpath(cls):  # noqa: N805
            return "$.result.custom_records[*]"

    custom_stream = CustomJsonPathJsonRpcStream(tap)

    # Create a response with data that matches the custom JSONPath
    response._content = json.dumps(
        {
            "jsonrpc": "2.0",
            "id": "123",
            "result": {
                "custom_records": [
                    {"id": 3, "name": "Custom 1"},
                    {"id": 4, "name": "Custom 2"},
                ]
            },
        }
    ).encode()

    # The custom jsonpath should extract from the nested location
    records = list(custom_stream.parse_response(response))
    assert len(records) == 2
    assert records[0]["id"] == 3
    assert records[1]["id"] == 4
