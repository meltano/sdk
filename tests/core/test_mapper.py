"""Test map transformer."""

import copy
import json
import logging
from typing import Dict, List, Optional, Set, cast

import pytest

from singer_sdk.exceptions import MapExpressionError
from singer_sdk.helpers._catalog import get_selected_schema
from singer_sdk.helpers._singer import Catalog
from singer_sdk.mapper import PluginMapper, RemoveRecordTransform, md5
from singer_sdk.streams.core import Stream
from singer_sdk.tap_base import Tap
from singer_sdk.typing import (
    IntegerType,
    ObjectType,
    PropertiesList,
    Property,
    StringType,
)


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
    ).to_dict()
    foobars_schema = PropertiesList(
        Property("the", StringType),
        Property("brown", StringType),
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
        ]
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
            },
            {
                "name": "my-tap-something",
                "owner_email": "sample2@example.com",
                "description": "Comment B",
            },
            {
                "name": "target-something",
                "owner_email": "sample3@example.com",
                "description": "Comment C",
            },
            {
                "name": "not-atap",
                "owner_email": "sample4@example.com",
                "description": "Comment D",
            },
        ],
        "foobars": [
            {"the": "quick"},
            {"brown": "fox"},
        ],
    }


# Transform cases


@pytest.fixture
def transform_stream_maps():
    return {
        "repositories": {
            # "__source__": "repositories",
            "repo_name": "_['name']",
            "email_domain": "owner_email.split('@')[1]",
            "email_hash": "md5(config['hash_seed'] + owner_email)",
            "description": "'[masked]'",
            "description2": "str('[masked]')",
            "int_test": "int('0')",
            "__else__": None,
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
                    stream_map_config["hash_seed"] + "sample1@example.com"
                ),
                "description": "[masked]",
                "description2": "[masked]",
                "int_test": 0,
            },
            {
                "repo_name": "my-tap-something",
                "email_domain": "example.com",
                "email_hash": md5(
                    stream_map_config["hash_seed"] + "sample2@example.com"
                ),
                "description": "[masked]",
                "description2": "[masked]",
                "int_test": 0,
            },
            {
                "repo_name": "target-something",
                "email_domain": "example.com",
                "email_hash": md5(
                    stream_map_config["hash_seed"] + "sample3@example.com"
                ),
                "description": "[masked]",
                "description2": "[masked]",
                "int_test": 0,
            },
            {
                "repo_name": "not-atap",
                "email_domain": "example.com",
                "email_hash": md5(
                    stream_map_config["hash_seed"] + "sample4@example.com"
                ),
                "description": "[masked]",
                "description2": "[masked]",
                "int_test": 0,
            },
        ],
        "foobars": [  # should be unchanged
            {"the": "quick"},
            {"brown": "fox"},
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
            Property("int_test", IntegerType),
        ).to_dict(),
        "foobars": PropertiesList(
            Property("the", StringType),
            Property("brown", StringType),
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
def cloned_and_aliased_result(stream_map_config, sample_stream):
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
    restult = copy.copy(filter_stream_maps)
    restult["repositories"]["__filter__"] = "this should raise an er!ror"
    return restult


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


def _test_transform(
    test_name: str,
    stream_maps,
    stream_map_config,
    expected_result,
    expected_schemas,
    sample_stream,
    sample_catalog_obj,
):
    output: Dict[str, List[dict]] = {}
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
                logging.info(f"Skipping ignored stream '{stream_name}'")
                continue

            assert (
                expected_schemas[stream_map.stream_alias]
                == stream_map.transformed_schema
            ), (
                f"Failed '{test_name}' schema test. Generated schema was "
                f"{json.dumps(stream_map.transformed_schema, indent=2)}"
            )

            output[stream_map.stream_alias] = []
            for record in stream:
                result = stream_map.transform(record)
                if result is None:
                    """Filter out record"""
                    continue

                output[stream_map.stream_alias].append(result)

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
            ),
        ),
    ).to_dict()

    def get_records(self, context):
        yield {
            "email": "alice@example.com",
            "count": 21,
            "user": {"id": 1, "sub": {"num": 1}},
        }
        yield {
            "email": "bob@example.com",
            "count": 13,
            "user": {"id": 2, "sub": {"num": 2}},
        }
        yield {
            "email": "charlie@example.com",
            "count": 19,
            "user": {"id": 3, "sub": {"num": 3}},
        }


class MappedTap(Tap):
    """A tap with mapped streams."""

    name = "tap-mapped"

    def discover_streams(self):
        """Discover streams."""
        return [MappedStream(self)]


@pytest.fixture
def clear_schema_cache() -> None:
    """Schemas are cached, so the cache needs to be cleared between test invocations."""
    yield
    get_selected_schema.cache_clear()


@pytest.mark.parametrize(
    "stream_alias,stream_maps,flatten,flatten_max_depth,output_fields,key_properties",
    [
        (
            "mystream",
            {},
            False,
            0,
            {"email", "count", "user"},
            [],
        ),
        (
            "mystream",
            {
                "mystream": {
                    "email_hash": "md5(email)",
                }
            },
            False,
            0,
            {"email", "count", "user", "email_hash"},
            [],
        ),
        (
            "mystream",
            {
                "mystream": {
                    "email_hash": "md5(email)",
                    "fixed_count": "int(count-1)",
                    "__else__": None,
                }
            },
            False,
            0,
            {"fixed_count", "email_hash"},
            [],
        ),
        (
            "mystream",
            {
                "mystream": {
                    "email_hash": "md5(email)",
                    "__key_properties__": ["email_hash"],
                    "__else__": None,
                }
            },
            False,
            0,
            {"email_hash"},
            ["email_hash"],
        ),
        (
            "sourced_stream_1",
            {"mystream": None, "sourced_stream_1": {"__source__": "mystream"}},
            False,
            0,
            {"email", "count", "user"},
            [],
        ),
        (
            "sourced_stream_2",
            {"sourced_stream_2": {"__source__": "mystream"}, "__else__": None},
            False,
            0,
            {"email", "count", "user"},
            [],
        ),
        (
            "aliased_stream",
            {"mystream": {"__alias__": "aliased_stream"}},
            False,
            0,
            {"email", "count", "user"},
            [],
        ),
        (
            "mystream",
            {},
            True,
            1,
            {"email", "count", "user__id", "user__sub"},
            [],
        ),
        (
            "mystream",
            {},
            True,
            10,
            {"email", "count", "user__id", "user__sub__num"},
            [],
        ),
        (
            "mystream",
            {
                "mystream": {
                    "email_hash": "md5(email)",
                    "__key_properties__": ["email_hash"],
                }
            },
            True,
            10,
            {"email", "count", "email_hash", "user__id", "user__sub__num"},
            ["email_hash"],
        ),
        (
            "mystream",
            {
                "mystream": {
                    "email": None,
                }
            },
            False,
            0,
            {"count", "user"},
            [],
        ),
    ],
    ids=[
        "no_map",
        "keep_all_fields",
        "only_mapped_fields",
        "changed_key_properties",
        "sourced_stream_1",
        "sourced_stream_2",
        "aliased_stream",
        "flatten_depth_1",
        "flatten_all",
        "map_and_flatten",
        "drop_property",
    ],
)
def test_mapped_stream(
    clear_schema_cache: None,
    stream_alias: str,
    stream_maps: dict,
    flatten: bool,
    flatten_max_depth: Optional[int],
    output_fields: Set[str],
    key_properties: List[str],
):
    tap = MappedTap(
        config={
            "stream_maps": stream_maps,
            "flattening_enabled": flatten,
            "flattening_max_depth": flatten_max_depth,
        }
    )
    stream = tap.streams["mystream"]

    schema_messages = list(stream._generate_schema_messages())
    assert len(schema_messages) == 1, "Incorrect number of schema messages generated."
    schema_message = schema_messages[0]
    assert schema_message.stream == stream_alias
    assert schema_message.key_properties == key_properties
    assert schema_message.schema["properties"].keys() == output_fields

    for raw_record in stream.get_records(None):
        record_message = next(
            stream._generate_record_messages(cast(dict, raw_record), None)
        )
        transformed_record = record_message.record

        assert record_message.stream == stream_alias
        assert output_fields == set(transformed_record.keys())

        for output_field in output_fields:
            assert transformed_record[
                output_field
            ], f"Value for field '{output_field}' should be nonempty."
