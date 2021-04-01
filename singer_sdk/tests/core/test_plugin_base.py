import os
from unittest import mock

from singer_sdk.typing import IntegerType, PropertiesList, Property, StringType
from singer_sdk.plugin_base import PluginBase


class PluginTest(PluginBase):
    """Example Plugin for tests."""

    name = "plugin-test"
    config_jsonschema = PropertiesList(
        Property("prop1", StringType, required=True),
        Property("prop2", IntegerType),
    ).to_dict()


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
