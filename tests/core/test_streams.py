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
    EndOfStreamError,
    FatalAPIError,
    InvalidReplicationKeyException,
    SingerSDKError,
    SkippableSyncError,
    SyncError,
)
from singer_sdk.helpers._compat import SingerSDKDeprecationWarning
from singer_sdk.helpers._compat import datetime_fromisoformat as parse
from singer_sdk.helpers.jsonpath import _compile_jsonpath
from singer_sdk.singerlib import Catalog, MetadataMapping
from singer_sdk.streams.core import REPLICATION_FULL_TABLE, REPLICATION_INCREMENTAL
from singer_sdk.streams.graphql import GraphQLStream
from singer_sdk.streams.rest import RESTStream
from singer_sdk.typing import IntegerType, PropertiesList, Property, StringType
from tests.core.conftest import SimpleTestStream

if t.TYPE_CHECKING:
    import requests_mock

    from singer_sdk import Stream, Tap
    from singer_sdk.helpers.types import Context, Record
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


def test_end_of_stream_error_skips_partition(tap: Tap):
    """Raising EndOfStreamError in get_records should skip that partition only.

    Verifies the partition-level try/except in Stream._sync_records: records
    from healthy partitions are emitted while the failing partition is skipped.
    """

    class PartitionedStream(SimpleTestStream):
        @property
        def partitions(self) -> list[dict]:  # type: ignore[override]
            return [{"repo": "good"}, {"repo": "broken"}, {"repo": "also-good"}]

        def get_records(  # type: ignore[override]
            self,
            context: dict | None,
        ) -> t.Generator[dict, None, None]:
            if context and context["repo"] == "broken":
                msg = "Simulated partition error."
                raise EndOfStreamError(msg)
            yield {"id": 1, "value": "test", "updatedAt": "2021-01-01T00:00:00Z"}

    stream = PartitionedStream(tap)
    records = list(stream._sync_records(write_messages=False))
    assert len(records) == 2  # good + also-good emitted; broken skipped


def test_end_of_stream_error_skips_stream(tap_class: type[SimpleTestTap]):
    """Raising EndOfStreamError in sync should skip that stream only.

    Verifies the stream-level try/except in Tap.sync_all: the failing stream
    is skipped while remaining streams continue to sync normally.
    """
    synced_streams: list[str] = []

    class GoodStream(SimpleTestStream):
        name = "good_stream"

        def get_records(  # type: ignore[override]
            self,
            context: dict | None,  # noqa: ARG002
        ) -> t.Generator[dict, None, None]:
            synced_streams.append(self.name)
            yield {"id": 1, "value": "test", "updatedAt": "2021-01-01T00:00:00Z"}

    class BrokenStream(SimpleTestStream):
        name = "broken_stream"

        def get_records(  # type: ignore[override]
            self,
            context: dict | None,  # noqa: ARG002
        ) -> t.Generator[dict, None, None]:
            msg = "Simulated stream error."
            raise EndOfStreamError(msg)
            yield  # noqa: unreachable

        def sync(self) -> None:  # type: ignore[override]
            msg = "Simulated stream error."
            raise EndOfStreamError(msg)

    class AnotherGoodStream(SimpleTestStream):
        name = "another_good_stream"

        def get_records(  # type: ignore[override]
            self,
            context: dict | None,  # noqa: ARG002
        ) -> t.Generator[dict, None, None]:
            synced_streams.append(self.name)
            yield {"id": 2, "value": "test", "updatedAt": "2021-01-01T00:00:00Z"}

    class MultiStreamTap(tap_class):
        def discover_streams(self) -> list[Stream]:
            return [GoodStream(self), BrokenStream(self), AnotherGoodStream(self)]

    tap = MultiStreamTap(
        config={
            "username": "utest",
            "password": "ptest",
            "start_date": "2021-01-01",
        },
        parse_env_config=False,
    )
    tap.sync_all()

    assert "good_stream" in synced_streams
    assert "another_good_stream" in synced_streams
    assert "broken_stream" not in synced_streams


def test_end_of_stream_error_empty_partition_list(tap: Tap):
    """Sync on a stream with empty partitions falls back to a single default context.

    Verifies that when partitions returns an empty list, the SDK treats the
    stream as unpartitioned and runs get_records once with an empty context,
    emitting records normally without raising.
    """

    class EmptyPartitionStream(SimpleTestStream):
        @property
        def partitions(self) -> list[dict]:  # type: ignore[override]
            """Return an empty partition list."""  # ruff: ignore[property-docstring-starts-with-verb]
            return []

        def get_records(  # type: ignore[override]
            self,
            context: dict | None,  # noqa: ARG002
        ) -> t.Generator[dict, None, None]:
            """Yield records normally."""
            yield {"id": 1, "value": "test", "updatedAt": "2021-01-01T00:00:00Z"}

    stream = EmptyPartitionStream(tap)
    records = list(stream._sync_records(write_messages=False))
    assert len(records) == 1


def test_end_of_stream_error_no_streams(tap_class: type[SimpleTestTap]):
    """sync_all on a tap with no streams should complete without error.

    Verifies that when discover_streams returns an empty list, sync_all
    exits cleanly without crashing.
    """

    class EmptyTap(tap_class):
        def discover_streams(self) -> list[Stream]:
            """Return no streams."""
            return []

    empty_tap = EmptyTap(
        config={
            "username": "utest",
            "password": "ptest",
            "start_date": "2021-01-01",
        },
        parse_env_config=False,
    )
    empty_tap.sync_all()  # should not raise


def test_end_of_stream_error_all_partitions_fail(tap: Tap):
    """EndOfStreamError on every partition should yield no records but not crash.

    Verifies that when all partitions raise EndOfStreamError, the tap exits
    cleanly with zero records emitted.
    """

    class AllFailStream(SimpleTestStream):
        @property
        def partitions(self) -> list[dict]:  # type: ignore[override]
            """Return three partitions that all fail."""  # ruff: ignore[property-docstring-starts-with-verb]
            return [{"repo": "a"}, {"repo": "b"}, {"repo": "c"}]

        def get_records(  # type: ignore[override]
            self,
            context: dict | None,  # noqa: ARG002
        ) -> t.Generator[dict, None, None]:
            """Always raise EndOfStreamError."""
            msg = "All partitions failed."
            raise EndOfStreamError(msg)
            yield  # noqa: unreachable

    stream = AllFailStream(tap)
    records = list(stream._sync_records(write_messages=False))
    assert records == []


def test_end_of_stream_error_first_partition_skipped(tap: Tap):
    """EndOfStreamError on the first partition should not prevent others from syncing.

    Verifies the continue correctly skips to the next iteration rather than
    breaking out of the loop entirely.
    """

    class FirstFailsStream(SimpleTestStream):
        @property
        def partitions(self) -> list[dict]:  # type: ignore[override]
            """Return partitions where the first one fails."""  # ruff: ignore[property-docstring-starts-with-verb]
            return [{"repo": "broken"}, {"repo": "good-1"}, {"repo": "good-2"}]

        def get_records(  # type: ignore[override]
            self,
            context: dict | None,
        ) -> t.Generator[dict, None, None]:
            """Raise on first partition only."""
            if context and context["repo"] == "broken":
                msg = "First partition failed."
                raise EndOfStreamError(msg)
            yield {"id": 1, "value": "test", "updatedAt": "2021-01-01T00:00:00Z"}

    stream = FirstFailsStream(tap)
    records = list(stream._sync_records(write_messages=False))
    assert len(records) == 2  # good-1 and good-2 emitted


def test_end_of_stream_error_last_partition_skipped(tap: Tap):
    """EndOfStreamError on the last partition should not cause off-by-one issues.

    Verifies that a continue on the final loop iteration exits cleanly with
    records from earlier partitions preserved.
    """

    class LastFailsStream(SimpleTestStream):
        @property
        def partitions(self) -> list[dict]:  # type: ignore[override]
            """Return partitions where the last one fails."""  # ruff: ignore[property-docstring-starts-with-verb]
            return [{"repo": "good-1"}, {"repo": "good-2"}, {"repo": "broken"}]

        def get_records(  # type: ignore[override]
            self,
            context: dict | None,
        ) -> t.Generator[dict, None, None]:
            """Raise on last partition only."""
            if context and context["repo"] == "broken":
                msg = "Last partition failed."
                raise EndOfStreamError(msg)
            yield {"id": 1, "value": "test", "updatedAt": "2021-01-01T00:00:00Z"}

    stream = LastFailsStream(tap)
    records = list(stream._sync_records(write_messages=False))
    assert len(records) == 2  # good-1 and good-2 emitted


def test_end_of_stream_error_only_partition_fails(tap: Tap):
    """EndOfStreamError on the sole partition should yield no records but not crash.

    Verifies that a single-partition stream that fails exits cleanly with
    zero records emitted.
    """

    class SingleFailStream(SimpleTestStream):
        @property
        def partitions(self) -> list[dict]:  # type: ignore[override]
            """Return a single partition that fails."""  # ruff: ignore[property-docstring-starts-with-verb]
            return [{"repo": "broken"}]

        def get_records(  # type: ignore[override]
            self,
            context: dict | None,  # noqa: ARG002
        ) -> t.Generator[dict, None, None]:
            """Always raise EndOfStreamError."""
            msg = "Only partition failed."
            raise EndOfStreamError(msg)
            yield  # noqa: unreachable

    stream = SingleFailStream(tap)
    records = list(stream._sync_records(write_messages=False))
    assert records == []


def test_non_end_of_stream_error_propagates_at_partition_level(tap: Tap):
    """Non-EndOfStreamError exceptions must not be swallowed by the partition catcher.

    Verifies that a plain RuntimeError raised in get_records() propagates
    through _sync_records and crashes the tap, confirming the except block
    is precise and does not accidentally catch unrelated errors.
    """

    class RuntimeErrorStream(SimpleTestStream):
        @property
        def partitions(self) -> list[dict]:  # type: ignore[override]
            """Return a single partition."""  # ruff: ignore[property-docstring-starts-with-verb]
            return [{"repo": "broken"}]

        def get_records(  # type: ignore[override]
            self,
            context: dict | None,  # noqa: ARG002
        ) -> t.Generator[dict, None, None]:
            """Raise a non-EndOfStreamError exception."""
            msg = "Unexpected error."
            raise RuntimeError(msg)
            yield  # noqa: unreachable

    stream = RuntimeErrorStream(tap)
    with pytest.raises(RuntimeError, match="Unexpected error."):  # ruff: ignore[pytest-raises-ambiguous-pattern]
        list(stream._sync_records(write_messages=False))


def test_non_end_of_stream_error_propagates_at_stream_level(
    tap_class: type[SimpleTestTap],
):
    """Non-EndOfStreamError exceptions must not be swallowed by the stream catcher.

    Verifies that a plain RuntimeError raised in stream.sync() propagates
    through sync_all and crashes the tap, confirming the stream-level except
    block is precise and does not accidentally catch unrelated errors.
    """

    class RuntimeErrorStream(SimpleTestStream):
        name = "runtime_error_stream"

        def sync(self) -> None:  # type: ignore[override]
            """Raise a non-EndOfStreamError exception."""
            msg = "Unexpected stream error."
            raise RuntimeError(msg)

    class RuntimeErrorTap(tap_class):
        def discover_streams(self) -> list[Stream]:
            """Return a stream that raises RuntimeError."""
            return [RuntimeErrorStream(self)]

    error_tap = RuntimeErrorTap(
        config={
            "username": "utest",
            "password": "ptest",
            "start_date": "2021-01-01",
        },
        parse_env_config=False,
    )
    with pytest.raises(RuntimeError, match="Unexpected stream error."):  # ruff: ignore[pytest-raises-ambiguous-pattern]
        error_tap.sync_all()


def test_end_of_stream_error_hierarchy():
    """EndOfStreamError must satisfy the expected inheritance chain."""

    assert issubclass(EndOfStreamError, SkippableSyncError)
    assert issubclass(EndOfStreamError, SyncError)
    assert issubclass(EndOfStreamError, SingerSDKError)
