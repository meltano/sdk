from __future__ import annotations

import json
import typing as t
from contextlib import nullcontext

import pytest
from click.testing import CliRunner

import singer_sdk.singerlib as singer
from singer_sdk import typing as th
from singer_sdk.exceptions import ConfigValidationError
from singer_sdk.mapper_base import InlineMapper


class DummyInlineMapper(InlineMapper):
    """A dummy inline mapper."""

    name = "mapper-dummy"
    config_jsonschema = th.PropertiesList(
        th.Property(
            "stream_maps",
            th.ObjectType(
                additional_properties=th.CustomType(
                    {
                        "type": ["object", "string", "null"],
                        "properties": {
                            "__filter__": {"type": ["string", "null"]},
                            "__source__": {"type": ["string", "null"]},
                            "__else__": {"type": ["null"]},
                            "__key_properties__": {
                                "type": ["array", "null"],
                                "items": {"type": "string"},
                            },
                        },
                        "additionalProperties": {"type": ["string", "null"]},
                    },
                ),
            ),
            required=True,
            title="Stream Maps",
            description="Stream maps",
        ),
    ).to_dict()

    def map_schema_message(
        self,
        message_dict: dict,
    ) -> t.Generator[singer.Message, None, None]:
        yield singer.SchemaMessage(
            stream=message_dict["stream"],
            schema=message_dict["schema"],
            key_properties=message_dict.get("key_properties", []),
        )

    def map_record_message(
        self,
        message_dict: dict,
    ) -> t.Generator[singer.Message, None, None]:
        yield singer.RecordMessage(
            stream=message_dict["stream"],
            record=message_dict["record"],
        )

    def map_state_message(
        self,
        message_dict: dict,
    ) -> t.Generator[singer.Message, None, None]:
        yield singer.StateMessage(
            value=message_dict["value"],
        )

    def map_activate_version_message(
        self,
        message_dict: dict,
    ) -> t.Generator[singer.Message, None, None]:
        yield singer.ActivateVersionMessage(
            stream=message_dict["stream"],
            version=message_dict["version"],
        )


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
        DummyInlineMapper(config=config_dict, validate_config=True)

    if isinstance(exc, pytest.ExceptionInfo):
        assert exc.value.errors == errors


def test_cli_help():
    """Test the CLI help message."""
    runner = CliRunner()
    result = runner.invoke(DummyInlineMapper.cli, ["--help"])
    assert result.exit_code == 0
    assert "Show this message and exit." in result.output


def test_cli_config_validation(tmp_path, caplog: pytest.LogCaptureFixture):
    """Test the CLI config validation."""
    runner = CliRunner()
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps({}))

    with caplog.at_level("ERROR"):
        result = runner.invoke(DummyInlineMapper.cli, ["--config", str(config_path)])

    assert result.exit_code == 1
    assert not result.stdout
    assert "'stream_maps' is a required property" in caplog.text
