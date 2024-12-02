from __future__ import annotations

import typing as t

import pytest

from singer_sdk.plugin_base import SDK_PACKAGE_NAME, MapperNotInitialized, PluginBase
from singer_sdk.typing import IntegerType, PropertiesList, Property, StringType

if t.TYPE_CHECKING:
    from pathlib import Path


class PluginTest(PluginBase):
    """Example Plugin for tests."""

    name = "plugin-test"
    config_jsonschema = PropertiesList(
        Property("prop1", StringType, required=True),
        Property("prop2", IntegerType),
    ).to_dict()


def test_config_path(tmp_path: Path):
    """Test that the config path is correctly set."""
    config_json = '{"prop1": "hello", "prop2": 123}'
    config_path = tmp_path / "config.json"
    config_path.write_text(config_json)

    with pytest.deprecated_call():
        plugin = PluginTest(config=config_path)

    assert plugin.config == {"prop1": "hello", "prop2": 123}


def test_invalid_config_type():
    """Test that invalid config types raise an error."""
    with pytest.raises(TypeError, match="Error parsing config of type 'tuple'"):
        PluginTest(config=(("prop1", "hello"), ("prop2", 123)))


def test_get_env_var_config(monkeypatch: pytest.MonkeyPatch):
    """Test settings parsing from environment variables."""
    monkeypatch.delenv("PLUGIN_TEST_PROP1", raising=False)
    monkeypatch.delenv("PLUGIN_TEST_PROP2", raising=False)
    monkeypatch.delenv("PLUGIN_TEST_PROP3", raising=False)
    monkeypatch.delenv("PLUGIN_TEST_PROP4", raising=False)

    with monkeypatch.context() as m:
        m.setenv("PLUGIN_TEST_PROP1", "hello")
        m.setenv("PLUGIN_TEST_PROP3", "not-a-tap-setting")
        m.setenv("PLUGIN_TEST_PROP4", "not-a-tap-setting")
        env_config = PluginTest._env_var_config
        assert env_config["prop1"] == "hello"
        assert "PROP1" not in env_config
        assert "prop2" not in env_config
        assert "PROP2" not in env_config
        assert "prop3" not in env_config
        assert "PROP3" not in env_config

    no_env_config = PluginTest._env_var_config
    assert "prop1" not in no_env_config
    assert "PROP1" not in env_config
    assert "prop2" not in no_env_config
    assert "PROP2" not in env_config
    assert "prop3" not in no_env_config
    assert "PROP3" not in env_config


def test_mapper_not_initialized():
    """Test that the mapper is not initialized before the plugin is started."""
    plugin = PluginTest(
        parse_env_config=False,
        validate_config=False,
    )
    with pytest.raises(MapperNotInitialized):
        _ = plugin.mapper


def test_supported_python_versions():
    """Test that supported python versions are correctly parsed."""
    assert PluginBase._get_supported_python_versions(SDK_PACKAGE_NAME)
