from __future__ import annotations

import json
import logging
import typing as t
from pathlib import Path

import pytest

import singer_sdk.typing as th
from singer_sdk.configuration._dict_config import (
    merge_config_sources,
    parse_environment_config,
)

if t.TYPE_CHECKING:
    from pytest_subtests import SubTests

CONFIG_JSONSCHEMA = th.PropertiesList(
    th.Property("prop1", th.StringType, required=True),
    th.Property("prop2", th.StringType),
    th.Property("prop3", th.ArrayType(th.StringType)),
    th.Property("prop4", th.IntegerType),
    th.Property("prop5", th.BooleanType),
    th.Property("prop6", th.ArrayType(th.IntegerType)),
    th.Property(
        "prop7",
        th.ObjectType(
            th.Property("sub_prop1", th.StringType),
            th.Property("sub_prop2", th.IntegerType),
        ),
    ),
).to_dict()


@pytest.fixture
def config_file1(tmpdir) -> str:
    filepath: str = tmpdir.join("file1.json")
    with Path(filepath).open("w", encoding="utf-8") as f:
        json.dump({"prop2": "from-file-1"}, f)

    return filepath


@pytest.fixture
def config_file2(tmpdir) -> str:
    filepath: str = tmpdir.join("file2.json")
    with Path(filepath).open("w", encoding="utf-8") as f:
        json.dump({"prop3": ["from-file-2"]}, f)

    return filepath


def test_get_env_var_config(
    monkeypatch: pytest.MonkeyPatch,
    subtests: SubTests,
    caplog: pytest.LogCaptureFixture,
):
    """Test settings parsing from environment variables."""
    with monkeypatch.context() as m:
        m.setenv("PLUGIN_TEST_PROP1", "hello")
        m.setenv("PLUGIN_TEST_PROP3", '["val1","val2"]')
        m.setenv("PLUGIN_TEST_PROP4", "123")
        m.setenv("PLUGIN_TEST_PROP5", "TRUE")
        m.setenv("PLUGIN_TEST_PROP6", "[1,2,3]")
        m.setenv("PLUGIN_TEST_PROP7", '{"sub_prop1": "hello", "sub_prop2": 123}')
        m.setenv("PLUGIN_TEST_PROP999", "not-a-tap-setting")
        env_config = parse_environment_config(CONFIG_JSONSCHEMA, "PLUGIN_TEST_")

        with subtests.test(msg="Parse string from environment"):
            assert env_config["prop1"] == "hello"

        with subtests.test(msg="Parse array from environment"):
            assert env_config["prop3"] == ["val1", "val2"]

        with subtests.test(msg="Parse integer from environment"):
            assert env_config["prop4"] == 123

        with subtests.test(msg="Parse boolean from environment"):
            assert env_config["prop5"] is True

        with subtests.test(msg="Parse array of integers from environment"):
            assert env_config["prop6"] == [1, 2, 3]

        with subtests.test(msg="Parse object from environment"):
            assert env_config["prop7"] == {"sub_prop1": "hello", "sub_prop2": 123}

        with subtests.test(msg="Ignore non-tap setting"):
            missing_props = {"PROP1", "prop2", "PROP2", "prop999", "PROP999"}
            assert not set.intersection(missing_props, env_config)

        m.setenv("PLUGIN_TEST_PROP3", "val1,val2")
        with (
            subtests.test(msg="Legacy array parsing"),
            caplog.at_level(logging.WARNING),
        ):
            parsed = parse_environment_config(CONFIG_JSONSCHEMA, "PLUGIN_TEST_")
            assert parsed["prop3"] == ["val1", "val2"]

            assert any(
                "Parsing array of the form 'x,y,z'" in log.message
                for log in caplog.records
            )

    no_env_config = parse_environment_config(CONFIG_JSONSCHEMA, "PLUGIN_TEST_")
    missing_props = {
        "prop1",
        "PROP1",
        "prop2",
        "PROP2",
        "prop3",
        "PROP3",
        "prop4",
        "PROP4",
        "prop5",
        "PROP5",
        "prop6",
        "PROP6",
        "prop999",
        "PROP999",
    }
    with subtests.test(msg="Ignore missing environment variables"):
        assert not set.intersection(missing_props, no_env_config)


def test_get_dotenv_config(tmp_path: Path):
    dotenv = tmp_path / ".env"
    dotenv.write_text("PLUGIN_TEST_PROP1=hello\n")
    dotenv_config = parse_environment_config(
        CONFIG_JSONSCHEMA,
        "PLUGIN_TEST_",
        dotenv_path=dotenv,
    )
    assert dotenv_config
    assert dotenv_config["prop1"] == "hello"


def test_merge_config_sources(
    config_file1,
    config_file2,
    monkeypatch: pytest.MonkeyPatch,
):
    """Test merging multiple configuration sources."""
    with monkeypatch.context() as m:
        m.setenv("PLUGIN_TEST_PROP1", "from-env")
        m.setenv("PLUGIN_TEST_PROP999", "not-a-tap-setting")
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
