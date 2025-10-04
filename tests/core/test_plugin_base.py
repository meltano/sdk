from __future__ import annotations

import logging
import typing as t

import pytest
import simplejson

from singer_sdk.plugin_base import (
    SDK_PACKAGE_NAME,
    MapperNotInitialized,
    PluginBase,
    SingerCommand,
    _ConfigInput,
)
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


def test_config_from_cli_args(tmp_path: Path):
    """Test that input is converted to a merged config dict."""
    config_paths = [
        tmp_path / "config1.json",
        tmp_path / "config2.json",
    ]
    config_paths[0].write_text('{"prop1": "hello", "prop2": 123}')
    config_paths[1].write_text('{"prop2": 456, "prop3": "world"}')
    config_input = _ConfigInput.from_cli_args(*config_paths)
    assert config_input.config == {"prop1": "hello", "prop2": 456, "prop3": "world"}
    assert not config_input.parse_env


def test_config_from_cli_args_env(tmp_path: Path):
    """Test that input is converted to a merged config dict and parse_env is true."""
    config_path = tmp_path / "config.json"
    config_path.write_text('{"prop1": "hello"}')
    config_input = _ConfigInput.from_cli_args(config_path, "ENV")
    assert config_input.config == {"prop1": "hello"}
    assert config_input.parse_env


def test_config_from_cli_args_invalid_file(tmp_path: Path):
    """Test that invalid file paths raise an error."""
    missing_path = tmp_path / "config.json"
    with pytest.raises(
        FileNotFoundError,
        match=r"File at '.*' was not found",
    ) as exc_info:
        _ConfigInput.from_cli_args(missing_path, "ENV")
    # Assert the error message includes the specific file path
    assert str(missing_path) in str(exc_info.value)


def test_config_from_cli_args_invalid_json(tmp_path):
    """Test that invalid JSON raises a JSONDecodeError."""
    config_path = tmp_path / "invalid.json"
    config_path.write_text('{"prop1": "hello", "prop2": 123')  # missing closing }
    with pytest.raises((simplejson.JSONDecodeError, ValueError)):
        _ConfigInput.from_cli_args(config_path)


def test_config_from_cli_args_non_dict_json(tmp_path):
    """Test that non-dict JSON raises a TypeError."""
    config_path = tmp_path / "not_a_dict.json"
    config_path.write_text('["not", "a", "dict"]')
    with pytest.raises(ValueError, match="dictionary update sequence"):
        _ConfigInput.from_cli_args(config_path)


def test_singer_command_excepthook(
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture,
):
    """Test that the excepthook is correctly set."""
    logger = logging.getLogger("test_logger")
    command = SingerCommand(name="test", logger=logger)

    exc_info = (Exception, Exception("test"), None)
    with caplog.at_level(logging.ERROR):
        command.excepthook(*exc_info)

    assert len(caplog.messages) == 1
    assert caplog.messages[0] == "test"
    assert "test" not in capsys.readouterr().err

    caplog.clear()

    exc_info = (RuntimeError, RuntimeError(), None)
    with caplog.at_level(logging.ERROR):
        command.excepthook(*exc_info)

    assert len(caplog.messages) == 1
    assert caplog.messages[0] == "RuntimeError"
    assert "RuntimeError" not in capsys.readouterr().err

    caplog.clear()

    exc_info = (KeyboardInterrupt, KeyboardInterrupt(), None)
    with caplog.at_level(logging.ERROR):
        command.excepthook(*exc_info)

    assert not caplog.messages
    assert "KeyboardInterrupt" in capsys.readouterr().err
