from __future__ import annotations

import typing as t

import pytest

from singer_sdk.helpers.capabilities import CapabilitiesEnum, TargetCapabilities
from singer_sdk.target_base import SQLTarget


class SQLTargetMock(SQLTarget):
    name = "sql-target-mock"

    def __init_subclass__(
        cls,
        *,
        capabilities: t.Iterable[CapabilitiesEnum],
        **kwargs: t.Any,
    ):
        super().__init_subclass__(**kwargs)
        cls.capabilities = [*capabilities]
        cls.config_jsonschema = {"properties": {}}


@pytest.mark.parametrize(
    "capabilities,expected_settings",
    [
        pytest.param([], set(), id="no capabilities"),
        pytest.param(
            [TargetCapabilities.TARGET_SCHEMA],
            {"default_target_schema"},
            id="default schema",
        ),
        pytest.param(
            [TargetCapabilities.HARD_DELETE],
            {"hard_delete"},
            id="hard delete",
        ),
    ],
)
def test_target_about_info(
    capabilities: list[CapabilitiesEnum],
    expected_settings: set[str],
):
    class MyTarget(SQLTargetMock, capabilities=capabilities):
        pass

    about = MyTarget._get_about_info()
    default_settings = {
        "add_record_metadata",
        "load_method",
        "batch_size_rows",
    }
    assert set(about.settings["properties"]) == expected_settings | default_settings
