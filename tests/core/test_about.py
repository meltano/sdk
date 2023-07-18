"""Test the AboutInfo class."""

from __future__ import annotations

import typing as t

import pytest

from singer_sdk.about import AboutFormatter, AboutInfo
from singer_sdk.helpers.capabilities import TapCapabilities

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
        supported_python_versions=["3.6", "3.7", "3.8"],
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
            },
            "required": ["api_key"],
        },
    )


@pytest.mark.snapshot()
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
