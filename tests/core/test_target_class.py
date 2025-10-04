from __future__ import annotations

import json
from contextlib import nullcontext

import pytest
from click.testing import CliRunner

from singer_sdk import SQLTarget
from singer_sdk import typing as th
from singer_sdk.exceptions import ConfigValidationError


class DummyTarget(SQLTarget):
    """A dummy target class."""

    name = "target-dummy"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "required_property",
            th.StringType,
            required=True,
        ),
        th.Property(
            "optional_property",
            th.StringType,
            required=False,
        ),
    ).to_dict()


@pytest.mark.parametrize(
    "config_dict,expectation,errors",
    [
        pytest.param(
            {},
            pytest.raises(ConfigValidationError, match="Config validation failed"),
            ["'required_property' is a required property"],
            id="missing_required_property",
        ),
        pytest.param(
            {"required_property": "test"},
            nullcontext(),
            [],
            id="valid_config",
        ),
    ],
)
def test_config_errors(config_dict: dict, expectation, errors: list[str]):
    with expectation as exc:
        DummyTarget(config=config_dict, validate_config=True)

    if isinstance(exc, pytest.ExceptionInfo):
        assert exc.value.errors == errors


def test_cli():
    """Test the CLI."""
    runner = CliRunner()
    result = runner.invoke(DummyTarget.cli, ["--help"])
    assert result.exit_code == 0
    assert "Show this message and exit." in result.output


def test_cli_config_validation(tmp_path, caplog: pytest.LogCaptureFixture):
    """Test the CLI config validation."""
    runner = CliRunner()
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps({}))
    with caplog.at_level("ERROR"):
        result = runner.invoke(DummyTarget.cli, ["--config", str(config_path)])
    assert result.exit_code == 1
    assert not result.stdout
    assert "'required_property' is a required property" in caplog.text
