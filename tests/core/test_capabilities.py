from __future__ import annotations

import warnings
from inspect import currentframe, getframeinfo

import pytest

from singer_sdk.helpers.capabilities import (
    CapabilitiesEnum,
    PluginCapabilities,
    TargetCapabilities,
    config_for_capabilities,
    tap_config_for_capabilities,
    target_config_for_capabilities,
)
from singer_sdk.sql.tap import sql_tap_config_for_capabilities
from singer_sdk.sql.target import sql_target_config_for_capabilities


class DummyCapabilitiesEnum(CapabilitiesEnum):
    """Simple capabilities enumeration."""

    MY_SUPPORTED_FEATURE = "supported"
    MY_DEPRECATED_FEATURE = "deprecated", "No longer supported."


def test_deprecated_capabilities():
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        _ = DummyCapabilitiesEnum.MY_SUPPORTED_FEATURE

    with pytest.warns(
        DeprecationWarning,
        match="is deprecated. No longer supported",
    ) as record:
        _ = DummyCapabilitiesEnum.MY_DEPRECATED_FEATURE

    warning = record.list[0]
    frameinfo = getframeinfo(currentframe())
    assert warning.lineno == frameinfo.lineno - 3
    assert warning.filename.endswith("test_capabilities.py")

    with pytest.warns(
        DeprecationWarning,
        match="is deprecated. No longer supported",
    ) as record:
        DummyCapabilitiesEnum("deprecated")

    warning = record.list[0]
    frameinfo = getframeinfo(currentframe())
    assert warning.lineno == frameinfo.lineno - 3
    assert warning.filename.endswith("test_capabilities.py")


def test_config_for_capabilities():
    assert config_for_capabilities([]) == {"type": "object", "properties": {}}

    schema = config_for_capabilities(
        [PluginCapabilities.STREAM_MAPS, PluginCapabilities.FLATTENING],
    )
    assert {"stream_maps", "stream_map_config", "faker_config"} <= set(
        schema["properties"],
    )
    assert "flattening_enabled" in schema["properties"]


def test_tap_config_for_capabilities():
    schema = tap_config_for_capabilities(
        [PluginCapabilities.ACTIVATE_VERSION, PluginCapabilities.BATCH],
    )
    assert "emit_activate_version_messages" in schema["properties"]
    batch = schema["properties"]["batch_config"]
    assert batch["required"] == ["encoding"]
    assert batch["properties"]["encoding"]["required"] == ["format"]


def test_target_config_for_capabilities():
    schema = target_config_for_capabilities(
        [
            PluginCapabilities.BATCH,
            PluginCapabilities.ACTIVATE_VERSION,
            TargetCapabilities.VALIDATE_RECORDS,
        ],
    )
    assert {
        "add_record_metadata",
        "load_method",
        "batch_size_rows",
        "process_activate_version_messages",
        "validate_records",
    } <= set(schema["properties"])
    assert "required" not in schema["properties"]["batch_config"]


def test_sql_config_for_capabilities():
    assert "use_singer_decimal" in sql_tap_config_for_capabilities([])["properties"]

    schema = sql_target_config_for_capabilities(
        [TargetCapabilities.TARGET_SCHEMA, TargetCapabilities.HARD_DELETE],
    )
    assert {"default_target_schema", "hard_delete", "load_method"} <= set(
        schema["properties"],
    )
    assert (
        "default_target_schema"
        not in sql_target_config_for_capabilities([])["properties"]
    )
