"""Test map transformer."""

import json
from typing import Dict, List
from singer_sdk.typing import PropertiesList, Property, StringType
import pytest
import logging

from singer import Catalog

from singer_sdk.mapper import Mapper, RemoveRecordTransform, md5


@pytest.fixture
def map_config() -> dict:
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
def transform_map():
    return {
        "streams": {
            "repositories": {
                # "__source__": "repositories",
                "repo_name": "_['name']",
                "email_domain": "owner_email.split('@')[1]",
                "email_hash": "md5(config['hash_seed'] + owner_email)",
                "__else__": None,
            },
        },
    }


@pytest.fixture
def transformed_result(map_config):
    return {
        "repositories": [
            {
                "repo_name": "tap-something",
                "email_domain": "example.com",
                "email_hash": md5(map_config["hash_seed"] + "sample1@example.com"),
            },
            {
                "repo_name": "my-tap-something",
                "email_domain": "example.com",
                "email_hash": md5(map_config["hash_seed"] + "sample2@example.com"),
            },
            {
                "repo_name": "target-something",
                "email_domain": "example.com",
                "email_hash": md5(map_config["hash_seed"] + "sample3@example.com"),
            },
            {
                "repo_name": "not-atap",
                "email_domain": "example.com",
                "email_hash": md5(map_config["hash_seed"] + "sample4@example.com"),
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
        ).to_dict(),
        "foobars": PropertiesList(
            Property("the", StringType),
            Property("brown", StringType),
        ).to_dict(),
    }


# Filter and alias cases


@pytest.fixture
def filter_map():
    return {
        "streams": {
            "repositories": {
                "__filter__": ("'tap-' in name or 'target-' in name"),
                "name": "_['name']",
                "__else__": None,
            },
            "__else__": None,
        },
    }


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
    map_config,
    sample_stream,
    sample_catalog_dict,
    transform_map,
    transformed_result,
    transformed_schemas,
):
    _test_transform(
        "transform",
        map_config=map_config,
        sample_stream=sample_stream,
        sample_catalog_dict=sample_catalog_dict,
        map_dict=transform_map,
        expected_result=transformed_result,
        expected_schemas=transformed_schemas,
    )


def test_filter_transforms(
    map_config,
    sample_stream,
    sample_catalog_dict,
    filter_map,
    filtered_result,
    filtered_schemas,
):
    _test_transform(
        "filter",
        map_config=map_config,
        sample_stream=sample_stream,
        sample_catalog_dict=sample_catalog_dict,
        map_dict=filter_map,
        expected_result=filtered_result,
        expected_schemas=filtered_schemas,
    )


def _test_transform(
    test_name: str,
    map_config,
    sample_stream,
    sample_catalog_dict,
    map_dict,
    expected_result,
    expected_schemas,
):
    output: Dict[str, List[dict]] = {}
    mapper = Mapper(
        tap_map=map_dict,
        map_config=map_config,
        raw_catalog=sample_catalog_dict,
        logger=logging,
    )

    for stream_name, stream in sample_stream.items():
        if isinstance(mapper.get_primary_mapper(stream_name), RemoveRecordTransform):
            logging.info(f"Skipping ignored stream '{stream_name}'")
            continue

        stream_map = mapper.get_primary_mapper(stream_name)
        assert expected_schemas[stream_name] == stream_map.transformed_schema, (
            f"Failed '{test_name}' schema test. Generated schema was "
            f"{json.dumps(stream_map.transformed_schema, indent=2)}"
        )

        output[stream_name] = []
        for record in stream:
            result = stream_map.transform(record)
            if result is None:
                """Filter out record"""
                continue

            output[stream_name].append(result)
    assert expected_result == output, (
        f"Failed '{test_name}' test. "
        f"Generated output was {json.dumps(output, indent=2)}"
    )
