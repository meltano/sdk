"""Stream tests."""

import logging
from typing import Any, Dict, Iterable, List, Optional, cast

import pendulum
import pytest
import requests

from singer_sdk.helpers._classproperty import classproperty
from singer_sdk.helpers._singer import Catalog, CatalogEntry, MetadataMapping
from singer_sdk.helpers.jsonpath import _compile_jsonpath
from singer_sdk.streams.core import (
    REPLICATION_FULL_TABLE,
    REPLICATION_INCREMENTAL,
    Stream,
)
from singer_sdk.streams.graphql import GraphQLStream
from singer_sdk.streams.rest import RESTStream
from singer_sdk.tap_base import Tap
from singer_sdk.typing import (
    DateTimeType,
    IntegerType,
    PropertiesList,
    Property,
    StringType,
)

CONFIG_START_DATE = "2021-01-01"


class SimpleTestStream(Stream):
    """Test stream class."""

    name = "test"
    schema = PropertiesList(
        Property("id", IntegerType, required=True),
        Property("value", StringType, required=True),
        Property("updatedAt", DateTimeType, required=True),
    ).to_dict()
    replication_key = "updatedAt"

    def __init__(self, tap: Tap):
        """Create a new stream."""
        super().__init__(tap, schema=self.schema, name=self.name)

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        """Generate records."""
        yield {"id": 1, "value": "Egypt"}
        yield {"id": 2, "value": "Germany"}
        yield {"id": 3, "value": "India"}


class UnixTimestampIncrementalStream(SimpleTestStream):
    name = "unix_ts"
    schema = PropertiesList(
        Property("id", IntegerType, required=True),
        Property("value", StringType, required=True),
        Property("updatedAt", IntegerType, required=True),
    ).to_dict()
    replication_key = "updatedAt"


class UnixTimestampIncrementalStream2(UnixTimestampIncrementalStream):
    name = "unix_ts_override"

    def compare_start_date(self, value: str, start_date_value: str) -> str:
        """Compare a value to a start date value."""

        start_timestamp = pendulum.parse(start_date_value).format("X")
        return max(value, start_timestamp, key=float)


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


class SimpleTestTap(Tap):
    """Test tap class."""

    name = "test-tap"
    settings_jsonschema = PropertiesList(Property("start_date", DateTimeType)).to_dict()

    def discover_streams(self) -> List[Stream]:
        """List all streams."""
        return [
            SimpleTestStream(self),
            UnixTimestampIncrementalStream(self),
            UnixTimestampIncrementalStream2(self),
        ]


@pytest.fixture
def tap() -> SimpleTestTap:
    """Tap instance."""
    return SimpleTestTap(
        config={"start_date": CONFIG_START_DATE},
        parse_env_config=False,
    )


@pytest.fixture
def stream(tap: SimpleTestTap) -> SimpleTestStream:
    """Create a new stream instance."""
    return cast(SimpleTestStream, tap.load_streams()[0])


@pytest.fixture
def unix_timestamp_stream(tap: SimpleTestTap) -> UnixTimestampIncrementalStream:
    """Create a new stream instance."""
    return cast(UnixTimestampIncrementalStream, tap.load_streams()[1])


def test_stream_apply_catalog(tap: SimpleTestTap, stream: SimpleTestStream):
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
                    }
                ]
            }
        )
    )

    assert stream.primary_keys == ["id"]
    assert stream.replication_key is None
    assert stream.replication_method == REPLICATION_FULL_TABLE
    assert stream.forced_replication_method == REPLICATION_FULL_TABLE


@pytest.mark.parametrize(
    "stream_name,bookmark_value,expected_starting_value",
    [
        pytest.param(
            "test",
            None,
            pendulum.parse(CONFIG_START_DATE),
            id="datetime-repl-key-no-state",
        ),
        pytest.param(
            "test",
            "2021-02-01",
            pendulum.datetime(2021, 2, 1),
            id="datetime-repl-key-recent-bookmark",
        ),
        pytest.param(
            "test",
            "2020-01-01",
            pendulum.parse(CONFIG_START_DATE),
            id="datetime-repl-key-old-bookmark",
        ),
        pytest.param(
            "unix_ts",
            None,
            CONFIG_START_DATE,
            id="naive-unix-ts-repl-key-no-state",
        ),
        pytest.param(
            "unix_ts",
            "1612137600",
            "1612137600",
            id="naive-unix-ts-repl-key-recent-bookmark",
        ),
        pytest.param(
            "unix_ts",
            "1577858400",
            "1577858400",
            id="naive-unix-ts-repl-key-old-bookmark",
        ),
        pytest.param(
            "unix_ts_override",
            None,
            CONFIG_START_DATE,
            id="unix-ts-repl-key-no-state",
        ),
        pytest.param(
            "unix_ts_override",
            "1612137600",
            "1612137600",
            id="unix-ts-repl-key-recent-bookmark",
        ),
        pytest.param(
            "unix_ts_override",
            "1577858400",
            pendulum.parse(CONFIG_START_DATE).format("X"),
            id="unix-ts-repl-key-old-bookmark",
        ),
    ],
)
def test_stream_starting_timestamp(
    tap: SimpleTestTap,
    stream_name: str,
    bookmark_value: str,
    expected_starting_value: Any,
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
                }
            }
        }
    )
    stream._write_starting_replication_value(None)
    assert get_starting_value(None) == expected_starting_value


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
def test_jsonpath_rest_stream(
    tap: SimpleTestTap, path: str, content: str, result: List[dict]
):
    """Validate records are extracted correctly from the API response."""
    fake_response = requests.Response()
    fake_response._content = str.encode(content)

    RestTestStream.records_jsonpath = path
    stream = RestTestStream(tap)

    rows = stream.parse_response(fake_response)

    assert list(rows) == result


def test_jsonpath_graphql_stream_default(tap: SimpleTestTap):
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
    rows = stream.parse_response(fake_response)

    assert list(rows) == [{"id": 1, "value": "abc"}, {"id": 2, "value": "def"}]


def test_jsonpath_graphql_stream_override(tap: SimpleTestTap):
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
        def records_jsonpath(cls):
            return "$[*]"

    stream = GraphQLJSONPathOverride(tap)

    rows = stream.parse_response(fake_response)

    assert list(rows) == [{"id": 1, "value": "abc"}, {"id": 2, "value": "def"}]


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
    tap: SimpleTestTap, path: str, content: str, headers: dict, result: str
):
    """Validate pagination token is extracted correctly from API response."""
    fake_response = requests.Response()
    fake_response.headers.update(headers)
    fake_response._content = str.encode(content)

    RestTestStream.next_page_token_jsonpath = path
    stream = RestTestStream(tap)

    next_page = stream.get_next_page_token(fake_response, previous_token=None)

    assert next_page == result


def test_cached_jsonpath():
    """Test compiled JSONPath is cached."""
    expression = "$[*]"
    compiled = _compile_jsonpath(expression)
    recompiled = _compile_jsonpath(expression)

    # cached objects should point to the same memory location
    assert recompiled is compiled


def test_sync_costs_calculation(tap: SimpleTestTap, caplog):
    """Test sync costs are added up correctly."""
    fake_request = requests.PreparedRequest()
    fake_response = requests.Response()

    stream = RestTestStream(tap)

    def calculate_test_cost(
        request: requests.PreparedRequest,
        response: requests.Response,
        context: Optional[Dict],
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
