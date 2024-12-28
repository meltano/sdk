from __future__ import annotations

import json
import typing as t
from contextlib import nullcontext

import pytest
from click.testing import CliRunner

from singer_sdk.exceptions import ConfigValidationError

if t.TYPE_CHECKING:
    from singer_sdk import Tap


@pytest.mark.parametrize(
    "config_dict,expectation,errors",
    [
        pytest.param(
            {},
            pytest.raises(ConfigValidationError, match="Config validation failed"),
            ["'username' is a required property", "'password' is a required property"],
            id="missing_username_and_password",
        ),
        pytest.param(
            {"username": "utest"},
            pytest.raises(ConfigValidationError, match="Config validation failed"),
            ["'password' is a required property"],
            id="missing_password",
        ),
        pytest.param(
            {"username": "utest", "password": "ptest", "extra": "not valid"},
            pytest.raises(ConfigValidationError, match="Config validation failed"),
            ["Additional properties are not allowed ('extra' was unexpected)"],
            id="extra_property",
        ),
        pytest.param(
            {"username": None, "password": "ptest"},
            pytest.raises(ConfigValidationError, match="Config validation failed"),
            ["None is not of type 'string' in config['username']"],
            id="null_username",
        ),
        pytest.param(
            {"username": "utest", "password": "ptest", "nested": {}},
            pytest.raises(ConfigValidationError, match="Config validation failed"),
            ["'key' is a required property in config['nested']"],
            id="missing_required_nested_key",
        ),
        pytest.param(
            {"username": "utest", "password": "ptest", "array": []},
            nullcontext(),
            [],
            id="empty_array",
        ),
        pytest.param(
            {"username": "utest", "password": "ptest", "array": [{}]},
            pytest.raises(ConfigValidationError, match="Config validation failed"),
            ["'key' is a required property in config['array'][0]"],
            id="array_with_empty_object",
        ),
        pytest.param(
            {"username": "utest", "password": "ptest"},
            nullcontext(),
            [],
            id="valid_config",
        ),
    ],
)
def test_config_errors(
    tap_class: type[Tap],
    config_dict: dict,
    expectation,
    errors: list[str],
):
    with expectation as exc:
        tap_class(config=config_dict, validate_config=True)

    if isinstance(exc, pytest.ExceptionInfo):
        assert exc.value.errors == errors


def test_cli(tap_class: type[Tap]):
    """Test the CLI."""
    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(tap_class.cli, ["--help"])
    assert result.exit_code == 0
    assert "Show this message and exit." in result.output


def test_cli_config_validation(tap_class: type[Tap], tmp_path):
    """Test the CLI config validation."""
    runner = CliRunner(mix_stderr=False)
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps({}))
    result = runner.invoke(tap_class.cli, ["--config", str(config_path)])
    assert result.exit_code == 1
    assert not result.stdout
    assert "'username' is a required property" in result.stderr
    assert "'password' is a required property" in result.stderr


def test_cli_discover(tap_class: type[Tap], tmp_path):
    """Test the CLI discover command."""
    runner = CliRunner(mix_stderr=False)
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps({}))
    result = runner.invoke(
        tap_class.cli,
        [
            "--config",
            str(config_path),
            "--discover",
        ],
    )
    assert result.exit_code == 0
    assert "streams" in json.loads(result.stdout)
