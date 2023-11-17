"""Test map transformer."""

from __future__ import annotations

import copy
import datetime
import io
import json
import logging
import typing as t
from contextlib import redirect_stdout
from decimal import Decimal

import pytest
import time_machine

from singer_sdk._singerlib import Catalog
from singer_sdk.exceptions import MapExpressionError
from singer_sdk.helpers._catalog import get_selected_schema
from singer_sdk.mapper import PluginMapper, RemoveRecordTransform, md5
from singer_sdk.streams.core import Stream
from singer_sdk.tap_base import Tap
from singer_sdk.typing import (
    ArrayType,
    BooleanType,
    IntegerType,
    NumberType,
    ObjectType,
    OneOf,
    PropertiesList,
    Property,
    StringType,
)

if t.TYPE_CHECKING:
    from pathlib import Path

    from pytest_snapshot.plugin import Snapshot


@pytest.fixture
def stream_map_config() -> dict:
    return {"hash_seed": "super_secret_hash_seed"}


# Sample input


@pytest.fixture
def sample_catalog_dict() -> dict:
    repositories_schema = PropertiesList(
        Property("name", StringType),
        Property("owner_email", StringType),
        Property("description", StringType),
        Property("description", StringType),
    ).to_dict()
    foobars_schema = PropertiesList(
        Property("the", StringType),
        Property("brown", StringType),
    ).to_dict()
    nested_jellybean_schema = PropertiesList(
        Property("id", IntegerType),
        Property(
            "custom_fields",
            ArrayType(
                ObjectType(
                    Property("id", IntegerType),
                    Property("value", OneOf(StringType, IntegerType, BooleanType)),
                ),
            ),
        ),
    ).to_dict()
    return {
        "streams": [
            {
                "stream": "repositories",
                "tap_stream_id": "repositories",
                "schema": repositories_schema,
            },
            {
                "stream": "foobars",
                "tap_stream_id": "foobars",
                "schema": foobars_schema,
            },
            {
                "stream": "nested_jellybean",
                "tap_stream_id": "nested_jellybean",
                "schema": nested_jellybean_schema,
            },
        ],
    }


@pytest.fixture
def sample_catalog_obj(sample_catalog_dict) -> Catalog:
    return Catalog.from_dict(sample_catalog_dict)


@pytest.fixture
def sample_stream():
    return {
        "repositories": [
            {
                "name": "tap-something",
                "owner_email": "sample1@example.com",
                "description": "Comment A",
                "create_date": "2019-01-01",
            },
            {
                "name": "my-tap-something",
                "owner_email": "sample2@example.com",
                "description": "Comment B",
                "create_date": "2020-01-01",
            },
            {
                "name": "target-something",
                "owner_email": "sample3@example.com",
                "description": "Comment C",
                "create_date": "2021-01-01",
            },
            {
                "name": "not-atap",
                "owner_email": "sample4@example.com",
                "description": "Comment D",
                "create_date": "2022-01-01",
            },
        ],
        "foobars": [
            {"the": "quick"},
            {"brown": "fox"},
        ],
        "nested_jellybean": [
            {
                "id": 123,
                "custom_fields": [
                    {"id": 1, "value": "abc"},
                    {"id": 2, "value": 1212},
                    {"id": 3, "value": None},
                ],
            },
            {
                "id": 124,
                "custom_fields": [
                    {"id": 1, "value": "foo"},
                    {"id": 2, "value": 9009},
                    {"id": 3, "value": True},
                ],
            },
        ],
    }


# Transform cases


@pytest.fixture
def transform_stream_maps():
    nested_jellybean_custom_field_1 = (
        'dict([(x["id"], x["value"]) for x in custom_fields]).get(1)'
    )
    nested_jellybean_custom_field_2 = (
        'int(dict([(x["id"], x["value"]) for x in custom_fields]).get(2)) '
        'if dict([(x["id"], x["value"]) for x in custom_fields]).get(2) '
        "else None"
    )
    nested_jellybean_custom_field_3 = (
        'bool(dict([(x["id"], x["value"]) for x in custom_fields]).get(3)) '
        'if dict([(x["id"], x["value"]) for x in custom_fields]).get(3) '
        "else None"
    )
    return {
        "repositories": {
            "repo_name": "_['name']",
            "email_domain": "owner_email.split('@')[1]",
            "email_hash": "md5(config['hash_seed'] + owner_email)",
            "description": "'[masked]'",
            "description2": "str('[masked]')",
            "create_year": "int(datetime.date.fromisoformat(create_date).year)",
            "int_test": "int('0')",
            "__else__": None,
        },
        "nested_jellybean": {
            "custom_fields": "__NULL__",
            "custom_field_1": nested_jellybean_custom_field_1,
            "custom_field_2": nested_jellybean_custom_field_2,
            "custom_field_3": nested_jellybean_custom_field_3,
        },
    }


@pytest.fixture
def transformed_result(stream_map_config):
    return {
        "repositories": [
            {
                "repo_name": "tap-something",
                "email_domain": "example.com",
                "email_hash": md5(
                    stream_map_config["hash_seed"] + "sample1@example.com",
                ),
                "description": "[masked]",
                "description2": "[masked]",
                "create_year": 2019,
                "int_test": 0,
            },
            {
                "repo_name": "my-tap-something",
                "email_domain": "example.com",
                "email_hash": md5(
                    stream_map_config["hash_seed"] + "sample2@example.com",
                ),
                "description": "[masked]",
                "description2": "[masked]",
                "create_year": 2020,
                "int_test": 0,
            },
            {
                "repo_name": "target-something",
                "email_domain": "example.com",
                "email_hash": md5(
                    stream_map_config["hash_seed"] + "sample3@example.com",
                ),
                "description": "[masked]",
                "description2": "[masked]",
                "create_year": 2021,
                "int_test": 0,
            },
            {
                "repo_name": "not-atap",
                "email_domain": "example.com",
                "email_hash": md5(
                    stream_map_config["hash_seed"] + "sample4@example.com",
                ),
                "description": "[masked]",
                "description2": "[masked]",
                "create_year": 2022,
                "int_test": 0,
            },
        ],
        "foobars": [  # should be unchanged
            {"the": "quick"},
            {"brown": "fox"},
        ],
        "nested_jellybean": [
            {
                "id": 123,
                "custom_field_1": "abc",
                "custom_field_2": 1212,
                "custom_field_3": None,
            },
            {
                "id": 124,
                "custom_field_1": "foo",
                "custom_field_2": 9009,
                "custom_field_3": True,
            },
        ],
    }


@pytest.fixture
def transformed_schemas():
    return {
        "repositories": PropertiesList(
            Property("repo_name", StringType),
            Property("email_domain", StringType),
            Property("email_hash", StringType),
            Property("description", StringType),
            Property("description2", StringType),
            Property("create_year", IntegerType),
            Property("int_test", IntegerType),
        ).to_dict(),
        "foobars": PropertiesList(
            Property("the", StringType),
            Property("brown", StringType),
        ).to_dict(),
        "nested_jellybean": PropertiesList(
            Property("id", IntegerType),
            Property("custom_field_1", StringType),
            Property("custom_field_2", IntegerType),
            Property("custom_field_3", BooleanType),
        ).to_dict(),
    }


# Clone and alias case


@pytest.fixture
def clone_and_alias_stream_maps():
    return {
        "repositories": {"__alias__": "repositories_aliased"},
        "repositories_clone_1": {"__source__": "repositories"},
        "repositories_clone_2": {"__source__": "repositories"},
        "__else__": None,
    }


@pytest.fixture
def cloned_and_aliased_result(sample_stream):
    return {
        "repositories_aliased": sample_stream["repositories"],
        "repositories_clone_1": sample_stream["repositories"],
        "repositories_clone_2": sample_stream["repositories"],
    }


@pytest.fixture
def cloned_and_aliased_schemas():
    properties = PropertiesList(
        Property("name", StringType),
        Property("owner_email", StringType),
        Property("description", StringType),
    ).to_dict()
    return {
        "repositories_aliased": properties,
        "repositories_clone_1": properties,
        "repositories_clone_2": properties,
    }


# Filter and alias cases


@pytest.fixture
def filter_stream_maps():
    return {
        "repositories": {
            "__filter__": ("'tap-' in name or 'target-' in name"),
            "name": "_['name']",
            "__else__": None,
        },
        "__else__": None,
    }


@pytest.fixture
def filter_stream_map_w_error(filter_stream_maps):
    result = copy.copy(filter_stream_maps)
    result["repositories"]["__filter__"] = "this should raise an er!ror"
    return result


@pytest.fixture
def filtered_result():
    return {
        "repositories": [
            {"name": "tap-something"},
            {"name": "my-tap-something"},
            {"name": "target-something"},
        ],
    }


@pytest.fixture
def filtered_schemas():
    return {"repositories": PropertiesList(Property("name", StringType)).to_dict()}


def test_map_transforms(
    sample_stream,
    sample_catalog_obj,
    transform_stream_maps,
    stream_map_config,
    transformed_result,
    transformed_schemas,
):
    _test_transform(
        "transform",
        stream_maps=transform_stream_maps,
        stream_map_config=stream_map_config,
        expected_result=transformed_result,
        expected_schemas=transformed_schemas,
        sample_stream=sample_stream,
        sample_catalog_obj=sample_catalog_obj,
    )


def test_clone_and_alias_transforms(
    sample_stream,
    sample_catalog_obj,
    clone_and_alias_stream_maps,
    stream_map_config,
    cloned_and_aliased_result,
    cloned_and_aliased_schemas,
):
    _test_transform(
        "clone_and_alias",
        stream_maps=clone_and_alias_stream_maps,
        stream_map_config=stream_map_config,
        expected_result=cloned_and_aliased_result,
        expected_schemas=cloned_and_aliased_schemas,
        sample_stream=sample_stream,
        sample_catalog_obj=sample_catalog_obj,
    )


def test_filter_transforms(
    sample_stream,
    sample_catalog_obj,
    filter_stream_maps,
    stream_map_config,
    filtered_result,
    filtered_schemas,
):
    _test_transform(
        "filter",
        stream_maps=filter_stream_maps,
        stream_map_config=stream_map_config,
        expected_result=filtered_result,
        expected_schemas=filtered_schemas,
        sample_stream=sample_stream,
        sample_catalog_obj=sample_catalog_obj,
    )


def test_filter_transforms_w_error(
    sample_stream,
    sample_catalog_obj,
    filter_stream_map_w_error,
    stream_map_config,
    filtered_result,
    filtered_schemas,
):
    with pytest.raises(MapExpressionError):
        _test_transform(
            "filter",
            stream_maps=filter_stream_map_w_error,
            stream_map_config=stream_map_config,
            expected_result=filtered_result,
            expected_schemas=filtered_schemas,
            sample_stream=sample_stream,
            sample_catalog_obj=sample_catalog_obj,
        )


def _run_transform(
    *,
    stream_maps,
    stream_map_config,
    sample_stream,
    sample_catalog_obj,
):
    output: dict[str, list[dict]] = {}
    output_schemas = {}
    mapper = PluginMapper(
        plugin_config={
            "stream_maps": stream_maps,
            "stream_map_config": stream_map_config,
        },
        logger=logging.getLogger(),
    )
    mapper.register_raw_streams_from_catalog(sample_catalog_obj)

    for stream_name, stream in sample_stream.items():
        for stream_map in mapper.stream_maps[stream_name]:
            if isinstance(stream_map, RemoveRecordTransform):
                logging.info("Skipping ignored stream '%s'", stream_name)
                continue
            output_schemas[stream_map.stream_alias] = stream_map.transformed_schema
            output[stream_map.stream_alias] = []
            for record in stream:
                result = stream_map.transform(record)
                if result is None:
                    """Filter out record"""
                    continue

                output[stream_map.stream_alias].append(result)
    return output, output_schemas


def _test_transform(
    test_name: str,
    *,
    stream_maps,
    stream_map_config,
    expected_result,
    expected_schemas,
    sample_stream,
    sample_catalog_obj,
):
    output, output_schemas = _run_transform(
        stream_maps=stream_maps,
        stream_map_config=stream_map_config,
        sample_stream=sample_stream,
        sample_catalog_obj=sample_catalog_obj,
    )

    assert set(expected_schemas.keys()) == set(output_schemas.keys()), (
        f"Failed `{test_name}` schema test. "
        f"'{set(expected_schemas.keys()) - set(output_schemas.keys())}' "
        "schemas not found. "
        f"'{set(output_schemas.keys()) - set(expected_schemas.keys())}' "
        "schemas not expected. "
    )
    for expected_schema_name, expected_schema in expected_schemas.items():
        output_schema = output_schemas[expected_schema_name]
        assert expected_schema == output_schema, (
            f"Failed '{test_name}' schema test. Generated schema was "
            f"{json.dumps(output_schema, indent=2)}"
        )

    assert expected_result == output, (
        f"Failed '{test_name}' record result test. "
        f"Generated output was {json.dumps(output, indent=2)}"
    )


class MappedStream(Stream):
    """A stream to be mapped."""

    name = "mystream"
    schema = PropertiesList(
        Property("email", StringType),
        Property("count", IntegerType),
        Property(
            "user",
            ObjectType(
                Property("id", IntegerType()),
                Property("sub", ObjectType(Property("num", IntegerType()))),
                Property("some_numbers", ArrayType(NumberType())),
            ),
        ),
    ).to_dict()

    def get_records(self, context):  # noqa: ARG002
        yield {
            "email": "alice@example.com",
            "count": 21,
            "user": {
                "id": 1,
                "sub": {"num": 1},
                "some_numbers": [Decimal("3.14"), Decimal("2.718")],
            },
        }
        yield {
            "email": "bob@example.com",
            "count": 13,
            "user": {
                "id": 2,
                "sub": {"num": 2},
                "some_numbers": [Decimal("10.32"), Decimal("1.618")],
            },
        }
        yield {
            "email": "charlie@example.com",
            "count": 19,
            "user": {
                "id": 3,
                "sub": {"num": 3},
                "some_numbers": [Decimal("1.414"), Decimal("1.732")],
            },
        }


class MappedTap(Tap):
    """A tap with mapped streams."""

    name = "tap-mapped"

    def discover_streams(self):
        """Discover streams."""
        return [MappedStream(self)]


@pytest.fixture
def _clear_schema_cache() -> None:
    """Schemas are cached, so the cache needs to be cleared between test invocations."""
    yield
    get_selected_schema.cache_clear()


@time_machine.travel(
    datetime.datetime(2022, 1, 1, tzinfo=datetime.timezone.utc),
    tick=False,
)
@pytest.mark.snapshot()
@pytest.mark.usefixtures("_clear_schema_cache")
@pytest.mark.parametrize(
    "stream_maps,flatten,flatten_max_depth,snapshot_name",
    [
        pytest.param(
            {},
            False,
            0,
            "no_map.jsonl",
            id="no_map",
        ),
        pytest.param(
            {
                "mystream": {
                    "email_hash": "md5(email)",
                },
            },
            False,
            0,
            "keep_all_fields.jsonl",
            id="keep_all_fields",
        ),
        pytest.param(
            {
                "mystream": {
                    "email_hash": "md5(email)",
                    "fixed_count": "int(count-1)",
                    "__else__": None,
                },
            },
            False,
            0,
            "only_mapped_fields.jsonl",
            id="only_mapped_fields",
        ),
        pytest.param(
            {
                "mystream": {
                    "email_hash": "md5(email)",
                    "fixed_count": "int(count-1)",
                    "__else__": "__NULL__",
                },
            },
            False,
            0,
            "only_mapped_fields_null_string.jsonl",
            id="only_mapped_fields_null_string",
        ),
        pytest.param(
            {
                "mystream": {
                    "email_hash": "md5(email)",
                    "__key_properties__": ["email_hash"],
                    "__else__": None,
                },
            },
            False,
            0,
            "changed_key_properties.jsonl",
            id="changed_key_properties",
        ),
        pytest.param(
            {"mystream": None, "sourced_stream_1": {"__source__": "mystream"}},
            False,
            0,
            "sourced_stream_1.jsonl",
            id="sourced_stream_1",
        ),
        pytest.param(
            {"mystream": "__NULL__", "sourced_stream_1": {"__source__": "mystream"}},
            False,
            0,
            "sourced_stream_1_null_string.jsonl",
            id="sourced_stream_1_null_string",
        ),
        pytest.param(
            {"sourced_stream_2": {"__source__": "mystream"}, "__else__": None},
            False,
            0,
            "sourced_stream_2.jsonl",
            id="sourced_stream_2",
        ),
        pytest.param(
            {"mystream": {"__alias__": "aliased_stream"}},
            False,
            0,
            "aliased_stream.jsonl",
            id="aliased_stream",
        ),
        pytest.param(
            {},
            True,
            0,
            "flatten_depth_0.jsonl",
            id="flatten_depth_0",
        ),
        pytest.param(
            {},
            True,
            1,
            "flatten_depth_1.jsonl",
            id="flatten_depth_1",
        ),
        pytest.param(
            {},
            True,
            10,
            "flatten_all.jsonl",
            id="flatten_all",
        ),
        pytest.param(
            {
                "mystream": {
                    "email_hash": "md5(email)",
                    "__key_properties__": ["email_hash"],
                },
            },
            True,
            10,
            "map_and_flatten.jsonl",
            id="map_and_flatten",
        ),
        pytest.param(
            {
                "mystream": {
                    "email": None,
                },
            },
            False,
            0,
            "drop_property.jsonl",
            id="drop_property",
        ),
        pytest.param(
            {"mystream": {"email": "__NULL__"}},
            False,
            0,
            "drop_property_null_string.jsonl",
            id="drop_property_null_string",
        ),
        pytest.param(
            {
                "mystream": {
                    "count": "count",
                    "__else__": None,
                },
            },
            False,
            0,
            "non_pk_passthrough.jsonl",
            id="non_pk_passthrough",
        ),
        pytest.param(
            {
                "mystream": {
                    "_data": "record",
                    "__else__": None,
                },
            },
            False,
            0,
            "record_to_column.jsonl",
            id="record_to_column",
        ),
    ],
)
def test_mapped_stream(
    snapshot: Snapshot,
    snapshot_dir: Path,
    stream_maps: dict,
    flatten: bool,
    flatten_max_depth: int | None,
    snapshot_name: str,
):
    snapshot.snapshot_dir = snapshot_dir.joinpath("mapped_stream")

    tap = MappedTap(
        config={
            "stream_maps": stream_maps,
            "flattening_enabled": flatten,
            "flattening_max_depth": flatten_max_depth,
        },
    )
    buf = io.StringIO()
    with redirect_stdout(buf):
        tap.sync_all()

    buf.seek(0)
    snapshot.assert_match(buf.read(), snapshot_name)


def test_bench_simple_map_transforms(
    benchmark,
    sample_stream,
    sample_catalog_dict,
    transform_stream_maps,
    stream_map_config,
):
    """Run benchmark tests using the "repositories" stream."""
    stream_size_scale = 1000

    repositories_catalog = {
        "streams": [
            x
            for x in sample_catalog_dict["streams"]
            if x["tap_stream_id"] == "repositories"
        ],
    }

    repositories_sample_stream = {
        "repositories": sample_stream["repositories"] * stream_size_scale,
    }
    repositories_transform_stream_maps = {
        "repositories": transform_stream_maps["repositories"],
    }
    repositories_sample_catalog_obj = Catalog.from_dict(repositories_catalog)
    benchmark(
        _run_transform,
        stream_maps=repositories_transform_stream_maps,
        stream_map_config=stream_map_config,
        sample_stream=repositories_sample_stream,
        sample_catalog_obj=repositories_sample_catalog_obj,
    )
