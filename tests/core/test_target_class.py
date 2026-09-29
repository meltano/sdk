from __future__ import annotations

import io
import json
import typing as t
from contextlib import nullcontext, redirect_stdout

import pytest
from click.testing import CliRunner

from singer_sdk import Target
from singer_sdk import typing as th
from singer_sdk.exceptions import ConfigValidationError
from singer_sdk.sql import SQLTarget


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


if t.TYPE_CHECKING:
    from pytest_snapshot.plugin import Snapshot


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


@pytest.mark.snapshot
def test_default_info(snapshot: Snapshot):
    """Test the default about info."""

    class BasicTarget(Target):
        """A basic target."""

        name = "target-example"
        package_name = "singer-sdk"

    buf = io.StringIO()
    with redirect_stdout(buf):
        BasicTarget.print_about(output_format="json")

    info = json.loads(buf.getvalue())
    # Environment-dependent values
    info["version"] = "<version>"
    info["sdk_version"] = "<sdk_version>"
    info["supported_python_versions"] = ["<python_version>"]

    snapshot.assert_match(json.dumps(info, indent=2) + "\n", "default_info.json")
