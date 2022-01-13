import json
import os
from unittest import mock

import pytest

import singer_sdk.typing as th
from singer_sdk.configuration._dict_config import (
    merge_config_sources,
    parse_environment_config,
)

CONFIG_JSONSCHEMA = th.PropertiesList(
    th.Property("prop1", th.StringType, required=True),
    th.Property("prop2", th.StringType),
    th.Property("prop3", th.ArrayType(th.StringType)),
).to_dict()


@pytest.fixture
def config_file1(tmpdir) -> str:
    filepath: str = tmpdir.join("file1.json")
    with open(filepath, "w") as f:
        json.dump({"prop2": "from-file-1"}, f)

    return filepath


@pytest.fixture
def config_file2(tmpdir) -> str:
    filepath: str = tmpdir.join("file2.json")
    with open(filepath, "w") as f:
        json.dump({"prop3": ["from-file-2"]}, f)

    return filepath


def test_get_env_var_config():
    """Test settings parsing from environment variables."""
    with mock.patch.dict(
        os.environ,
        {
            "PLUGIN_TEST_PROP1": "hello",
            "PLUGIN_TEST_PROP3": "val1,val2",
            "PLUGIN_TEST_PROP4": "not-a-tap-setting",
        },
    ):
        env_config = parse_environment_config(CONFIG_JSONSCHEMA, "PLUGIN_TEST_")
        assert env_config["prop1"] == "hello"
        assert env_config["prop3"] == ["val1", "val2"]
        assert "PROP1" not in env_config
        assert "prop2" not in env_config and "PROP2" not in env_config
        assert "prop4" not in env_config and "PROP4" not in env_config

    no_env_config = parse_environment_config(CONFIG_JSONSCHEMA, "PLUGIN_TEST_")
    assert "prop1" not in no_env_config and "PROP1" not in env_config
    assert "prop2" not in no_env_config and "PROP2" not in env_config
    assert "prop3" not in no_env_config and "PROP3" not in env_config
    assert "prop4" not in no_env_config and "PROP4" not in env_config


def test_get_env_var_config_not_parsable():
    """Test settings parsing from environment variables with a non-parsable value."""
    with mock.patch.dict(
        os.environ,
        {
            "PLUGIN_TEST_PROP1": "hello",
            "PLUGIN_TEST_PROP3": '["repeated"]',
        },
    ):
        with pytest.raises(ValueError):
            parse_environment_config(CONFIG_JSONSCHEMA, "PLUGIN_TEST_")


def test_merge_config_sources(config_file1, config_file2):
    """Test merging multiple configuration sources."""
    with mock.patch.dict(
        os.environ,
        {
            "PLUGIN_TEST_PROP1": "from-env",
            "PLUGIN_TEST_PROP4": "not-a-tap-setting",
        },
    ):
        config = merge_config_sources(
            [config_file1, config_file2, "ENV"],
            CONFIG_JSONSCHEMA,
            "PLUGIN_TEST_",
        )
        assert config["prop1"] == "from-env"
        assert config["prop2"] == "from-file-1"
        assert config["prop3"] == ["from-file-2"]
        assert "prop4" not in config


def test_merge_config_sources_missing_file():
    """Test merging multiple configuration sources when a file is not found."""
    with pytest.raises(FileNotFoundError):
        merge_config_sources(
            ["missing.json"],
            CONFIG_JSONSCHEMA,
            "PLUGIN_TEST_",
        )
