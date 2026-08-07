"""Tests for the `schema` setting acting as an alias for `default_target_schema`.

See https://github.com/meltano/sdk/issues/2766. When `default_target_schema` is
not configured, an explicit `schema` setting should be honored instead of the
SDK deriving a schema name from the incoming stream ID.
"""

from __future__ import annotations

import typing as t

import pytest

from singer_sdk.sql import SQLConnector, SQLSink, SQLTarget


class _Connector(SQLConnector):
    """Minimal connector that never actually connects."""


class _Sink(SQLSink):
    connector_class = _Connector


class _Target(SQLTarget):
    """Target exposing both `schema` and `default_target_schema` settings."""

    name = "target-schema-alias"
    config_jsonschema: t.ClassVar[dict] = {
        "type": "object",
        "properties": {
            "sqlalchemy_url": {"type": "string"},
            "schema": {"type": "string"},
            "default_target_schema": {"type": "string"},
        },
    }
    default_sink_class = _Sink


SCHEMA: dict = {
    "type": "object",
    "properties": {
        "id": {"type": "integer"},
        "name": {"type": "string"},
    },
}


def _make_sink(stream_name: str, **config: str) -> _Sink:
    target = _Target(config={"sqlalchemy_url": "sqlite:///", **config})
    return _Sink(
        target,
        stream_name=stream_name,
        schema=SCHEMA,
        key_properties=["id"],
    )


class TestSchemaSettingAlias:
    """Resolution order: default_target_schema > schema > stream-derived > None."""

    @pytest.mark.parametrize(
        "stream_name, config, expected_schema_name",
        [
            # `schema` is honored when default_target_schema is absent.
            pytest.param(
                "TAP_SCHEMA-users",
                {"schema": "target_schema"},
                "target_schema",
                id="schema-overrides-stream-derived-2-part",
            ),
            pytest.param(
                "mydb-TAP_SCHEMA-users",
                {"schema": "target_schema"},
                "target_schema",
                id="schema-overrides-stream-derived-3-part",
            ),
            pytest.param(
                "users",
                {"schema": "target_schema"},
                "target_schema",
                id="schema-used-for-single-part-stream",
            ),
            # default_target_schema always wins when both are set.
            pytest.param(
                "TAP_SCHEMA-users",
                {"schema": "ignored", "default_target_schema": "default_schema"},
                "default_schema",
                id="default-wins-over-schema",
            ),
            # Only default_target_schema set -> unchanged behavior.
            pytest.param(
                "TAP_SCHEMA-users",
                {"default_target_schema": "default_schema"},
                "default_schema",
                id="only-default-target-schema",
            ),
            # Neither set -> stream-derived behavior preserved.
            pytest.param(
                "TAP_SCHEMA-users",
                {},
                "tap_schema",
                id="no-config-stream-derived-2-part",
            ),
            pytest.param(
                "mydb-TAP_SCHEMA-users",
                {},
                "tap_schema",
                id="no-config-stream-derived-3-part",
            ),
            pytest.param(
                "users",
                {},
                None,
                id="no-config-single-part-returns-none",
            ),
            pytest.param(
                "a-b-c-d",
                {},
                None,
                id="no-config-four-part-returns-none",
            ),
        ],
    )
    def test_schema_name_resolution(
        self,
        stream_name: str,
        config: dict,
        expected_schema_name: str | None,
    ):
        sink = _make_sink(stream_name, **config)
        assert sink.schema_name == expected_schema_name

    def test_empty_schema_falls_through_to_stream_derived(self):
        """An empty-string `schema` is falsy and should not override."""
        sink = _make_sink("TAP_SCHEMA-users", schema="")
        assert sink.schema_name == "tap_schema"

    def test_schema_config_property_reads_setting(self):
        """`schema_config` reflects the raw `schema` setting."""
        assert _make_sink("users", schema="target_schema").schema_config == (
            "target_schema"
        )
        assert _make_sink("users").schema_config is None

    def test_schema_value_returned_verbatim(self):
        """`schema` is returned as-is (not conformed), like default_target_schema."""
        sink = _make_sink("TAP_SCHEMA-users", schema="MixedCase_Schema")
        assert sink.schema_name == "MixedCase_Schema"

    def test_table_name_unaffected_by_schema_setting(self):
        """The `schema` setting must not change how the table name is derived."""
        sink = _make_sink("TAP_SCHEMA-users", schema="target_schema")
        assert sink.table_name == "users"
