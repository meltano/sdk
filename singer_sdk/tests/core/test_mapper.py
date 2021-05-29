"""Test map transformer"""

from hashlib import md5 as _md5
import pytest

from singer_sdk.mapper import Mapper


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
            "repo_names": {
                "__source__": "repositories",
                "__filter__": ("'-tap-' in f'-{name}' or '-target-' in f'-{name}'"),
                "repo_name": "_['name']",
                "__else__": None,
            },
            "repositories": None,
        },
    }


@pytest.fixture
def filter_result():
    return {
        "repo_names": [
            {"repo_name": "tap-something"},
            # {"repo_name": "my-tap-something"},
            # {"repo_name": "target-something"},
            # {"repo_name": "not-atap"},
        ],
        "foobars": [  # should be unchanged
            {"the": "quick"},
            {"brown": "fox"},
        ],
    }


# Transform cases


@pytest.fixture
def transform_map():
    return {
        "streams": {
            "repo_names": {
                "__source__": "repositories",
                "repo_name": "_['name']",
                "email_domain": ["owner_email.split('@')[1]", "'Error parsing.'"],
                "email_hash": ["md5(config['hash_seed'] + owner_email)"],
                "__else__": None,
            },
            "repositories": None,
        },
    }


@pytest.fixture
def transform_result(config):
    def md5(x) -> str:
        return str(_md5(x.encode("utf-8")))

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
                "email_domain": "Error parsing.",
                "email_hash": md5(config["hash_seed"] + "sample4@example.com"),
            },
        ],
        "foobars": [  # should be unchanged
            {"the": "quick"},
            {"brown": "fox"},
        ],
    }


def test_map_transforms(sample_stream, config, transform_map, transform_result):
    _test_transform(sample_stream, config, transform_map, transform_result)


def test_filter_transforms(sample_stream, config, filter_map, filter_result):
    _test_transform(sample_stream, config, filter_map, filter_result)


def _test_transform(sample_stream, config, map_dict, result_dict):
    output = {}
    for stream_name, stream in sample_stream.items():
        mapper = Mapper(map_dict, config)
        output[stream_name] = []
        for record in stream:
            result = mapper.apply_default_mapper(stream_name, record)
            if result is None:
                """Filter out record"""
                continue

            output[stream_name].append(result)
    assert output == result_dict
