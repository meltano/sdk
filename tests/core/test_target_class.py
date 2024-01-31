from __future__ import annotations

import json
from contextlib import nullcontext

import pytest
from click.testing import CliRunner

from samples.sample_target_sqlite import SQLiteTarget
from singer_sdk.exceptions import ConfigValidationError


@pytest.mark.parametrize(
    "config_dict,expectation,errors",
    [
        pytest.param(
            {},
            pytest.raises(ConfigValidationError, match="Config validation failed"),
            ["'path_to_db' is a required property"],
            id="missing_path_to_db",
        ),
        pytest.param(
            {"path_to_db": "sqlite://test.db"},
            nullcontext(),
            [],
            id="valid_config",
        ),
    ],
)
def test_config_errors(config_dict: dict, expectation, errors: list[str]):
    with expectation as exc:
        SQLiteTarget(config=config_dict, validate_config=True)

    if isinstance(exc, pytest.ExceptionInfo):
        assert exc.value.errors == errors


def test_cli():
    """Test the CLI."""
    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(SQLiteTarget.cli, ["--help"])
    assert result.exit_code == 0
    assert "Show this message and exit." in result.output


def test_cli_config_validation(tmp_path):
    """Test the CLI config validation."""
    runner = CliRunner(mix_stderr=False)
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps({}))
    result = runner.invoke(SQLiteTarget.cli, ["--config", str(config_path)])
    assert result.exit_code == 1
    assert not result.stdout
    assert "'path_to_db' is a required property" in result.stderr
