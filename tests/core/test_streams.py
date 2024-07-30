"""Stream tests."""

from __future__ import annotations

import datetime
import logging
import typing as t

import pytest
import requests

from singer_sdk._singerlib import Catalog, MetadataMapping
from singer_sdk.exceptions import (
    InvalidReplicationKeyException,
)
from singer_sdk.helpers._classproperty import classproperty
from singer_sdk.helpers._compat import datetime_fromisoformat as parse
from singer_sdk.helpers.jsonpath import _compile_jsonpath, extract_jsonpath
from singer_sdk.pagination import first
from singer_sdk.streams.core import REPLICATION_FULL_TABLE, REPLICATION_INCREMENTAL
from singer_sdk.streams.graphql import GraphQLStream
from singer_sdk.streams.rest import RESTStream
from singer_sdk.typing import IntegerType, PropertiesList, Property, StringType
from tests.core.conftest import SimpleTestStream

if t.TYPE_CHECKING:
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

    def get_next_page_token(
        self,
        response: requests.Response,
        previous_token: str | None,  # noqa: ARG002
    ) -> str | None:
        if not self.next_page_token_jsonpath:
            return response.headers.get("X-Next-Page", None)

        all_matches = extract_jsonpath(
            self.next_page_token_jsonpath,
            response.json(),
        )
        try:
            return first(all_matches)
        except StopIteration:
            return None


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
                  "releation": "previous",
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

    with pytest.warns(DeprecationWarning):
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
