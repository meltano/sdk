"""Test the AboutInfo class."""

from __future__ import annotations

import typing as t

import pytest

from singer_sdk.helpers.capabilities import TapCapabilities
from singer_sdk.internal.about import AboutFormatter, AboutInfo

if t.TYPE_CHECKING:
    from pathlib import Path

    from pytest_snapshot.plugin import Snapshot


@pytest.fixture(scope="module")
def about_info() -> AboutInfo:
    return AboutInfo(
        name="tap-example",
        description="Example tap for Singer SDK",
        version="0.1.1",
        sdk_version="1.0.0",
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
    snapshot.assert_match(output, about_format)
