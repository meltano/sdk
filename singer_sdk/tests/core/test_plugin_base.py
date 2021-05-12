import contextlib
import datetime
import json
import os
from tempfile import NamedTemporaryFile
from typing import List
from unittest import mock

from pydantic import BaseModel
from pydantic.fields import Field

from singer_sdk.typing import IntegerType, PropertiesList, Property, StringType
from singer_sdk.plugin_base import PluginBase


class PluginTest(PluginBase):
    """Example Plugin for tests."""

    name = "plugin-test"
    config_jsonschema = PropertiesList(
        Property("prop1", StringType, required=True),
        Property("prop2", IntegerType),
    ).to_dict()


class NestedConfig(BaseModel):
    id: int
    start_date: datetime.datetime


class PydanticPlugin(PluginBase):
    """Example Pydantic Plugin for tests."""

    name = "pydantic-test"

    class ConfigModel(PluginBase.ConfigModel):
        """Pydantic Plugin config."""

        token: str
        some_flag: bool = False
        segments: List[int] = Field(default_factory=list)
        nested: NestedConfig = None

        class Config(PluginBase.ConfigModel.Config):
            env_prefix = "pydantic_test_"


def test_get_env_var_config():
    """Test settings parsing from environment variables."""

    with mock.patch.dict(
        os.environ,
        {
            "PLUGIN_TEST_PROP1": "hello",
            "PLUGIN_TEST_PROP3": "not-a-tap-setting",
        },
    ):
        env_config = PluginTest._env_var_config
        assert env_config["prop1"] == "hello"
        assert "PROP1" not in env_config
        assert "prop2" not in env_config and "PROP2" not in env_config
        assert "prop3" not in env_config and "PROP3" not in env_config

    no_env_config = PluginTest._env_var_config
    assert "prop1" not in no_env_config and "PROP1" not in env_config
    assert "prop2" not in no_env_config and "PROP2" not in env_config
    assert "prop3" not in no_env_config and "PROP3" not in env_config


def test_parse_env_using_pydantic_config():
    """Test Pydantic settings parsing from environment variables."""

    with mock.patch.dict(
        os.environ,
        {
            "PYDANTIC_TEST_TOKEN": "hello",
            "PYDANTIC_TEST_SEGMENTS": "[1,2,3]",
            "PYDANTIC_TEST_NESTED": '{"id": 1, "start_date": "2021-01-01T00:00:00"}',
        },
    ):
        plugin = PydanticPlugin(config={"some_flag": True}, parse_env_config=True)

    assert plugin.config == {
        "token": "hello",
        "some_flag": True,
        "segments": [1, 2, 3],
        "nested": {"id": 1, "start_date": datetime.datetime(2021, 1, 1)},
    }


def test_parse_dict_using_pydantic_config():
    """Test Pydantic settings parsing from a dictionary."""

    plugin = PydanticPlugin(config={"token": "abc", "some_flag": True})

    assert plugin.config == {
        "token": "abc",
        "some_flag": True,
        "segments": [],
        "nested": None,
    }


def test_parse_file_using_pydantic_config():
    """Test Pydantic settings parsing from a JSON file."""

    config = {"token": "abc", "some_flag": True}

    with NamedTemporaryFile(mode="w") as f:
        json.dump(config, f)
        f.seek(0)
        plugin = PydanticPlugin(config=f.name)

    assert plugin.config == {
        "token": "abc",
        "some_flag": True,
        "segments": [],
        "nested": None,
    }


def test_parse_file_list_using_pydantic_config():
    """Test Pydantic settings parsing from multiple JSON files."""

    configs = [
        {"token": "abc"},
        {"some_flag": True},
        {"token": "def"},
        {"segments": [1, 2]},
    ]

    paths = []

    with contextlib.ExitStack() as s:
        for c in configs:
            f = s.enter_context(NamedTemporaryFile(mode="w"))
            json.dump(c, f)
            f.seek(0)

            paths.append(f.name)

        plugin = PydanticPlugin(config=paths)

    assert plugin.config == {
        "token": "def",
        "some_flag": True,
        "segments": [1, 2],
        "nested": None,
    }
