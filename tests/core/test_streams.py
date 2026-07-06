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

from singer_sdk.exceptions import (
    FatalAPIError,
    InvalidReplicationKeyException,
)
from singer_sdk.helpers._compat import SingerSDKDeprecationWarning
from singer_sdk.helpers._compat import datetime_fromisoformat as parse
from singer_sdk.helpers.jsonpath import _compile_jsonpath
from singer_sdk.singerlib import Catalog, MetadataMapping
from singer_sdk.streams.core import REPLICATION_FULL_TABLE, REPLICATION_INCREMENTAL
from singer_sdk.streams.graphql import GraphQLStream
from singer_sdk.streams.rest import RESTStream
from singer_sdk.typing import IntegerType, PropertiesList, Property, StringType
from tests.core.conftest import SimpleTestStream, SimpleTestTap

if t.TYPE_CHECKING:
    import requests_mock

    from singer_sdk import Stream, Tap
    from singer_sdk.helpers.types import Context, Record

from unittest.mock import patch

from singer_sdk.helpers._util import get_nested_value

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
def stream(tap: SimpleTestTap):
    """Create a new stream instance."""
    return tap.load_streams()[0]


@pytest.mark.parametrize("no_replication_key", [None, "", False])
def test_stream_apply_catalog(
    stream: Stream,
    no_replication_key: t.Literal["", False] | None,
):
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
                        "replication_key": no_replication_key,
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


def test_jsonpath_graphql_stream_override(tap: Tap) -> None:
    """Validate graphql jsonpath can be updated."""
    content = """[
                        {"id": 1, "value": "abc"},
                        {"id": 2, "value": "def"}
                    ]
            """

    fake_response = requests.Response()
    fake_response._content = str.encode(content)

    class GraphQLJSONPathOverride(GraphqlTestStream):
        records_jsonpath = "$[*]"

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


def test_sync_costs_calculation(tap: Tap, caplog: pytest.LogCaptureFixture):
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
def test_stream_class_selection(
    tap_class: type[SimpleTestTap],
    input_catalog: dict[str, list[t.Any]] | dict[str, list[dict[str, t.Any]]] | None,
    selection: dict[str, bool],
):
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


def test_post_process_drops_record(tap: Tap):
    """Test post-processing is applied to records."""

    class DropsRecord(SimpleTestStream):
        def post_process(
            self,
            record: Record,
            context: Context | None,  # noqa: ARG002
        ) -> Record | None:
            # Drop even IDs
            return None if record["id"] % 2 == 0 else record

    stream = DropsRecord(tap)
    records = list(stream._sync_records(None, write_messages=False))
    assert records == [
        {"id": 1, "value": "Egypt", "updatedAt": "2021-01-01T00:00:00Z"},
        {"id": 3, "value": "India", "updatedAt": "2021-01-01T00:00:02Z"},
    ]


def test_post_process_transforms_record(tap: Tap):
    """Test post-processing is applied to records."""

    class TransformsRecord(SimpleTestStream):
        def post_process(
            self,
            record: Record,
            context: Context | None,  # noqa: ARG002
        ) -> Record | None:
            record["extra"] = "transformed"
            return record

    stream = TransformsRecord(tap)
    records = stream._sync_records(None, write_messages=False)
    assert all(record["extra"] == "transformed" for record in records)


@pytest.mark.parametrize(
    "keys,expected_context",
    [
        pytest.param(
            ["parent_id"],
            {"parent_id": 123},
            id="single_key",
        ),
        pytest.param(
            ["parent_id", "other_key"],
            {"parent_id": 123, "other_key": "abc"},
            id="multiple_keys",
        ),
        pytest.param(
            ["parent_id", "missing_key"],
            {"parent_id": 123},
            id="missing_keys",
        ),
        pytest.param(
            (),
            {},
            id="empty_tuple",
        ),
        pytest.param(
            [],
            {},
            id="empty_list",
        ),
        pytest.param(
            None,
            {"parent_id": 123, "other_key": "abc"},
            id="none",
        ),
    ],
)
def test_state_partitioning_keys_class_variable(
    tap: Tap,
    keys: t.Sequence[str] | None,
    expected_context: Context | None,
):
    """Regression test: class-level state_partitioning_keys=[] is respected.

    When a stream sets state_partitioning_keys=... as a class variable, the
    state_manager must receive the right value (not the default None,
    None). Bug: state_manager was initialised with self._state_partitioning_keys
    (always None from __init__) rather than self.state_partitioning_keys (which
    resolves the class attribute). See https://github.com/meltano/sdk/issues/3631.
    """

    class NoPartitionStream(SimpleTestStream):
        name = "no_partition"
        state_partitioning_keys = keys

    stream = NoPartitionStream(tap)
    original_context = {"parent_id": 123, "other_key": "abc"}
    assert (
        stream.state_manager.get_state_partition_context(original_context)
        == expected_context
    )


class _FakeStateManager:
    """Simple state manager stub that records increment_state calls."""

    def __init__(self) -> None:
        self.calls: list[tuple[dict, str]] = []

    def increment_state(
        self,
        latest_record: dict,
        *,
        replication_key: str,
        _context: dict | None = None,
    ) -> None:
        """Record the arguments passed to increment_state for test assertions."""
        self.calls.append((latest_record, replication_key))


"""Tests for nested replication key support - Issue #1198."""

SALESFORCE_RECORD = {
    "Id": "0015g00000XyZaAAB",
    "Name": "Acme Corp",
    "SystemModstamp": "2024-03-15T10:30:00.000Z",  # flat key — existing behavior
    "attributes": {
        "type": "Account",
        "updated": "2024-03-15T10:30:00.000Z",  # nested key — new behavior
    },
}

HUBSPOT_RECORD = {
    "id": "701",
    "properties": {
        "createdate": "2024-01-01T00:00:00.000Z",
        "hs_lastmodifieddate": "2024-06-01T08:45:22.000Z",  # nested timestamp
        "dealname": "Enterprise Deal",
    },
}

GITHUB_RECORD = {
    "id": 188271,
    "title": "Support nested replication keys",
    "updated_at": "2024-06-15T14:22:10Z",  # flat key — existing behavior
    "user": {
        "login": "daria-hrabar",
        "updated_at": "2024-05-01T09:00:00Z",  # nested key — new behavior
    },
}

DEEP_NESTED_RECORD = {
    "id": 42,
    "meta": {
        "audit": {
            "last_modified": "2024-04-20T16:00:00Z",  # 3 levels deep nested key
        }
    },
}


def test_flat_replication_key_unchanged() -> None:
    """Flat keys must behave exactly as before — no regression."""
    result = get_nested_value(GITHUB_RECORD, "updated_at")
    assert result == "2024-06-15T14:22:10Z"


def test_two_level_nested_key() -> None:
    """Dotted path traverses two levels to retrieve a nested timestamp."""
    result = get_nested_value(HUBSPOT_RECORD, "properties.hs_lastmodifieddate")
    assert result == "2024-06-01T08:45:22.000Z"


def test_two_level_nested_key_salesforce() -> None:
    """Dotted path works for Salesforce-style nested attributes."""
    result = get_nested_value(SALESFORCE_RECORD, "attributes.updated")
    assert result == "2024-03-15T10:30:00.000Z"


def test_three_level_nested_key() -> None:
    """Dotted path traverses three levels."""
    result = get_nested_value(DEEP_NESTED_RECORD, "meta.audit.last_modified")
    assert result == "2024-04-20T16:00:00Z"


# Edge case tests


def test_missing_final_key_returns_none() -> None:
    """Returns None when the final key does not exist."""
    result = get_nested_value(HUBSPOT_RECORD, "properties.nonexistent_field")
    assert result is None


def test_missing_intermediate_key_returns_none() -> None:
    """Returns None when an intermediate key does not exist."""
    result = get_nested_value(GITHUB_RECORD, "organization.updated_at")
    assert result is None


def test_intermediate_value_not_a_dict_returns_none() -> None:
    """Returns None when an intermediate value is a string, not a dict."""
    record = {"attributes": "corrupted-string"}
    result = get_nested_value(record, "attributes.updated")
    assert result is None


def test_empty_record_returns_none() -> None:
    """Returns None when the record is completely empty."""
    result = get_nested_value({}, "attributes.updated")
    assert result is None


def test_none_intermediate_value_returns_none() -> None:
    """Returns None when an intermediate key exists but holds None."""
    record = {"properties": None}
    result = get_nested_value(record, "properties.hs_lastmodifieddate")
    assert result is None


def test_get_nested_value_integer_intermediate_returns_none() -> None:
    """Returns None when intermediate value is an integer, not a dict."""
    record = {"attributes": 42}
    result = get_nested_value(record, "attributes.updated")
    assert result is None


def test_invalid_nested_replication_key_raises(tap: SimpleTestTap) -> None:
    """InvalidReplicationKeyException raised when nested key missing from schema."""

    class NestedKeyInvalidStream(SimpleTestStream):
        """Stream with a nested replication key not present in schema."""

        replication_key = "attributes.nonexistent"

    stream = NestedKeyInvalidStream(tap)

    with pytest.raises(
        InvalidReplicationKeyException,
        match=(
            f"Field '{stream.replication_key}' is not in schema for stream "
            f"'{stream.name}'"
        ),
    ):
        _ = stream.is_timestamp_replication_key


def test_increment_stream_state_flat_key(tap: SimpleTestTap) -> None:
    """Flat replication keys pass the original record unchanged to state manager."""
    stream = tap.streams["test"]
    fake_state_manager = _FakeStateManager()

    record = {"id": 1, "value": "x", "updatedAt": "2024-06-15T14:22:10Z"}

    with patch.object(stream, "state_manager", fake_state_manager):
        stream._increment_stream_state(record, context=None)

    assert len(fake_state_manager.calls) == 1
    passed_record, passed_key = fake_state_manager.calls[0]
    assert passed_record == record
    assert passed_key == "updatedAt"


def test_increment_stream_state_nested_key(tap: SimpleTestTap) -> None:
    """Nested replication keys pass a flattened record to the state manager."""

    class NestedStream(SimpleTestStream):
        """Stream with a nested replication key for testing state increment."""

        replication_key = "meta.audit.last_modified"

    stream = NestedStream(tap)
    fake_state_manager = _FakeStateManager()

    record = {"id": 1, "meta": {"audit": {"last_modified": "2024-04-20T16:00:00Z"}}}

    with patch.object(stream, "state_manager", fake_state_manager):
        stream._increment_stream_state(record, context=None)

    assert len(fake_state_manager.calls) == 1
    passed_record, passed_key = fake_state_manager.calls[0]
    assert passed_record == {"meta.audit.last_modified": "2024-04-20T16:00:00Z"}
    assert passed_key == "meta.audit.last_modified"
