"""Stream tests."""

from __future__ import annotations

import datetime
import decimal
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
)
from singer_sdk.helpers._classproperty import classproperty
from singer_sdk.helpers._compat import SingerSDKDeprecationWarning
from singer_sdk.helpers._compat import datetime_fromisoformat as parse
from singer_sdk.helpers.jsonpath import _compile_jsonpath
from singer_sdk.streams.core import REPLICATION_FULL_TABLE, REPLICATION_INCREMENTAL
from singer_sdk.streams.graphql import GraphQLStream
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
