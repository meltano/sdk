from __future__ import annotations

import io
import json
import typing as t
from contextlib import nullcontext, redirect_stdout

import pytest
from click.testing import CliRunner

from singer_sdk import Tap
from singer_sdk.exceptions import ConfigValidationError

if t.TYPE_CHECKING:
    from pytest_snapshot.plugin import Snapshot


@pytest.mark.parametrize(
    "config_dict,expectation,errors",
    [
        pytest.param(
            {},
            pytest.raises(ConfigValidationError, match="Config validation failed"),
            [
                "'password' is a required property",
                "'username' is a required property",
            ],
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
    runner = CliRunner()
    result = runner.invoke(tap_class.cli, ["--help"])
    assert result.exit_code == 0
    assert "Show this message and exit." in result.output


def test_cli_config_validation(
    tap_class: type[Tap],
    tmp_path,
    caplog: pytest.LogCaptureFixture,
):
    """Test the CLI config validation."""
    runner = CliRunner()
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps({}))
    with caplog.at_level("ERROR"):
        result = runner.invoke(tap_class.cli, ["--config", str(config_path)])
    assert result.exit_code == 1
    assert not result.stdout
    assert "'username' is a required property" in caplog.text
    assert "'password' is a required property" in caplog.text


def test_cli_discover(tap_class: type[Tap], tmp_path):
    """Test the CLI discover command."""
    runner = CliRunner()
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


@pytest.mark.snapshot
def test_default_info(snapshot: Snapshot):
    """Test the default about info."""

    class BasicTap(Tap):
        """A basic tap."""

        name = "tap-example"
        package_name = "singer-sdk"

    buf = io.StringIO()
    with redirect_stdout(buf):
        BasicTap.print_about(output_format="json")

    info = json.loads(buf.getvalue())
    # Environment-dependent values
    info["version"] = "<version>"
    info["sdk_version"] = "<sdk_version>"
    info["supported_python_versions"] = ["<python_version>"]

    snapshot.assert_match(json.dumps(info, indent=2) + "\n", "default_info.json")
