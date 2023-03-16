"""External tests fixtures."""

from __future__ import annotations

import json
from pathlib import Path

import pytest


def gitlab_config() -> dict | None:
    """Create a tap-gitlab config object."""

    path = Path("singer_sdk/tests/external/.secrets/gitlab-config.json")
    if not path.exists():
        # local testing relative path
        path = Path("tests/external/.secrets/gitlab-config.json")

    if path.exists():
        return json.loads(path.read_text())

    return None


@pytest.fixture(name="gitlab_config")
def gitlab_config_fixture() -> dict | None:
    return gitlab_config()


def ga_config() -> dict | None:
    """Create a tap-google-analytics config object."""
    path = Path("singer_sdk/tests/external/.secrets/google-analytics-config.json")

    if path.exists():
        return json.loads(path.read_text())

    return None


@pytest.fixture(name="ga_config")
def ga_config_fixture() -> dict | None:
    return ga_config()
