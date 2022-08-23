from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest
from click.testing import CliRunner

from singer_sdk.exceptions import ConfigValidationError

if TYPE_CHECKING:
    from singer_sdk import Tap


@pytest.mark.parametrize(
    "config_dict,errors",
    [
        (
            {},
            ["'username' is a required property", "'password' is a required property"],
        ),
        (
            {"username": "utest"},
            ["'password' is a required property"],
        ),
        (
            {"username": "utest", "password": "ptest", "extra": "not valid"},
            ["Additional properties are not allowed ('extra' was unexpected)"],
        ),
    ],
    ids=[
        "missing_username",
        "missing_password",
        "extra_property",
    ],
)
def test_config_errors(tap_class: type[Tap], config_dict: dict, errors: list[str]):
    with pytest.raises(ConfigValidationError, match="Config validation failed") as exc:
        tap_class(config_dict, validate_config=True)

    assert exc.value.errors == errors


def test_cli(tap_class: type[Tap], tmp_path):
    """Test the CLI."""
    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(tap_class.cli, ["--help"])
    assert result.exit_code == 0
    assert "Show this message and exit." in result.output

    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps({}))
    result = runner.invoke(tap_class.cli, ["--config", str(config_path)])
    assert result.exit_code == 1
    assert result.stdout == ""
    assert "'username' is a required property" in result.stderr
    assert "'password' is a required property" in result.stderr

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
    assert result.stderr == ""
