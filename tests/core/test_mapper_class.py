from __future__ import annotations

import json
from contextlib import nullcontext

import pytest
from click.testing import CliRunner

from samples.sample_mapper.mapper import StreamTransform
from singer_sdk.exceptions import ConfigValidationError


@pytest.mark.parametrize(
    "config_dict,expectation,errors",
    [
        pytest.param(
            {},
            pytest.raises(ConfigValidationError, match="Config validation failed"),
            ["'stream_maps' is a required property"],
            id="missing_stream_maps",
        ),
        pytest.param(
            {"stream_maps": {}},
            nullcontext(),
            [],
            id="valid_config",
        ),
    ],
)
def test_config_errors(config_dict: dict, expectation, errors: list[str]):
    with expectation as exc:
        StreamTransform(config=config_dict, validate_config=True)

    if isinstance(exc, pytest.ExceptionInfo):
        assert exc.value.errors == errors


def test_cli_help():
    """Test the CLI help message."""
    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(StreamTransform.cli, ["--help"])
    assert result.exit_code == 0
    assert "Show this message and exit." in result.output


def test_cli_config_validation(tmp_path):
    """Test the CLI config validation."""
    runner = CliRunner(mix_stderr=False)
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps({}))
    result = runner.invoke(StreamTransform.cli, ["--config", str(config_path)])
    assert result.exit_code == 1
    assert not result.stdout
    assert "'stream_maps' is a required property" in result.stderr
