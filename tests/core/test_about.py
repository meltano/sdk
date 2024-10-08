"""Test the AboutInfo class."""

from __future__ import annotations

import typing as t
from importlib import metadata

import pytest

from singer_sdk.about import (
    _PY_MAX_VERSION,
    _PY_MIN_VERSION,
    AboutFormatter,
    AboutInfo,
    get_supported_pythons,
)
from singer_sdk.helpers.capabilities import TapCapabilities
from singer_sdk.plugin_base import SDK_PACKAGE_NAME

if t.TYPE_CHECKING:
    from pathlib import Path

    from pytest_snapshot.plugin import Snapshot

_format_to_extension = {
    "text": "txt",
    "json": "json",
    "markdown": "md",
}


@pytest.fixture(scope="module")
def about_info() -> AboutInfo:
    return AboutInfo(
        name="tap-example",
        description="Example tap for Singer SDK",
        version="0.1.1",
        sdk_version="1.0.0",
        supported_python_versions=["3.8", "3.9", "3.10", "3.11", "3.12", "3.13"],
        capabilities=[
            TapCapabilities.CATALOG,
            TapCapabilities.DISCOVER,
            TapCapabilities.STATE,
        ],
        settings={
            "properties": {
                "start_date": {
                    "type": "string",
                    "format": "date-time",
                    "description": "Start date for the tap to extract data from.",
                },
                "api_key": {
                    "type": "string",
                    "description": "API key for the tap to use.",
                },
                "complex_setting": {
                    "type": "object",
                    "description": "A complex setting, with sub-settings.",
                    "properties": {
                        "sub_setting": {
                            "type": "string",
                            "description": "A sub-setting.",
                        }
                    },
                },
            },
            "required": ["api_key"],
        },
        env_var_prefix="TAP_EXAMPLE_",
    )


@pytest.mark.snapshot
@pytest.mark.parametrize(
    "about_format",
    [
        "text",
        "json",
        "markdown",
    ],
)
def test_about_format(
    snapshot: Snapshot,
    snapshot_dir: Path,
    about_info: AboutInfo,
    about_format: str,
):
    snapshot.snapshot_dir = snapshot_dir.joinpath("about_format")

    formatter = AboutFormatter.get_formatter(about_format)
    output = formatter.format_about(about_info)
    snapshot_name = f"{about_format}.snap.{_format_to_extension[about_format]}"
    snapshot.assert_match(output, snapshot_name)


def test_get_supported_pythons_sdk():
    package_metadata = metadata.metadata(SDK_PACKAGE_NAME)
    requires_python = package_metadata["Requires-Python"]

    supported_pythons = list(get_supported_pythons(requires_python))
    assert supported_pythons[0] == f"3.{_PY_MIN_VERSION}"
    assert supported_pythons[-1] == f"3.{_PY_MAX_VERSION}"


@pytest.mark.parametrize(
    "specifiers,expected",
    [
        (">=3.7,<3.12", ["3.7", "3.8", "3.9", "3.10", "3.11"]),
        (">=3.7", ["3.7", "3.8", "3.9", "3.10", "3.11", "3.12", "3.13"]),
        (">3.7", ["3.8", "3.9", "3.10", "3.11", "3.12", "3.13"]),
        (">3.7,<=3.11", ["3.8", "3.9", "3.10", "3.11"]),
    ],
)
def test_get_supported_pythons(specifiers: str, expected: list[str]):
    supported_pythons = list(get_supported_pythons(specifiers))
    assert supported_pythons == expected
