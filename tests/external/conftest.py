"""External tests fixtures."""

import json
from pathlib import Path
from typing import Optional

import pytest


def gitlab_config() -> Optional[dict]:
    """Create a tap-gitlab config object."""
    config: Optional[dict] = None

    path = Path("singer_sdk/tests/external/.secrets/gitlab-config.json")
    if not path.exists():
        # local testing relative path
        path = Path("tests/external/.secrets/gitlab-config.json")

    if path.exists():
        config = json.loads(path.read_text())

    return config


@pytest.fixture(name="gitlab_config")
def gitlab_config_fixture() -> Optional[dict]:
    return gitlab_config()


def ga_config() -> Optional[dict]:
    """Create a tap-google-analytics config object."""
    config: Optional[dict] = None
    path = Path("singer_sdk/tests/external/.secrets/google-analytics-config.json")

    if path.exists():
        config = json.loads(path.read_text())

    return config


@pytest.fixture(name="ga_config")
def ga_config_fixture() -> Optional[dict]:
    return ga_config()
