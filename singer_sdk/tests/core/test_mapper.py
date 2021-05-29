"""Test map transformer"""

import json
import pytest
import logging

from singer_sdk.mapper import Mapper, RemoveRecordTransform, md5


@pytest.fixture
def config() -> dict:
    return {"hash_seed": "super_secret_hash_seed"}


# Sample input


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
def transform_result(config):
    return {
        "repositories": [
            {
                "repo_name": "tap-something",
                "email_domain": "example.com",
                "email_hash": md5(config["hash_seed"] + "sample1@example.com"),
            },
            {
                "repo_name": "my-tap-something",
                "email_domain": "example.com",
                "email_hash": md5(config["hash_seed"] + "sample2@example.com"),
            },
            {
                "repo_name": "target-something",
                "email_domain": "example.com",
                "email_hash": md5(config["hash_seed"] + "sample3@example.com"),
            },
            {
                "repo_name": "not-atap",
                "email_domain": "example.com",
                "email_hash": md5(config["hash_seed"] + "sample4@example.com"),
            },
        ],
        "foobars": [  # should be unchanged
            {"the": "quick"},
            {"brown": "fox"},
        ],
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
def filter_result():
    return {
        "repositories": [
            {"name": "tap-something"},
            {"name": "my-tap-something"},
            {"name": "target-something"},
        ],
    }


def test_map_transforms(config, sample_stream, transform_map, transform_result):
    _test_transform("transform", config, sample_stream, transform_map, transform_result)


def test_filter_transforms(config, sample_stream, filter_map, filter_result):
    _test_transform("filter", config, sample_stream, filter_map, filter_result)


def _test_transform(test_name: str, config, sample_stream, map_dict, expected_result):
    output = {}
    mapper = Mapper(map_dict, config)
    for stream_name, stream in sample_stream.items():
        if isinstance(mapper.get_default_mapper(stream_name), RemoveRecordTransform):
            logging.info(f"Skipping ignored stream '{stream_name}'")
            continue

        output[stream_name] = []
        for record in stream:
            result = mapper.apply_default_mapper(stream_name, record)
            if result is None:
                """Filter out record"""
                continue

            output[stream_name].append(result)
    assert expected_result == output, (
        f"Failed '{test_name}' test. "
        f"Generated output was {json.dumps(output, indent=2)}"
    )
