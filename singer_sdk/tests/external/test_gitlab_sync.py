"""Test sample sync."""

import json
from pathlib import Path
from typing import Optional

from singer_sdk.samples.sample_tap_gitlab.gitlab_tap import SampleTapGitlab

COUNTER = 0


CONFIG_FILE = "singer_sdk/tests/external/.secrets/gitlab-config.json"
SAMPLE_CONFIG_BAD = {"not": "correct"}

config: Optional[dict] = None
if Path(CONFIG_FILE).exists():
    config = json.loads(Path(CONFIG_FILE).read_text())


def test_gitlab_sync_projects():
    """Test sync_one() for gitlab sample."""
    tap = SampleTapGitlab(config=config, parse_env_config=True)
    tap.sync_one("projects")


def test_gitlab_sync_commits():
    """Test sync_one() for gitlab sample."""
    tap = SampleTapGitlab(config=config, parse_env_config=True)
    tap.sync_one("commits")


def test_gitlab_sync_issues():
    """Test sync_one() for gitlab sample."""
    tap = SampleTapGitlab(config=config, parse_env_config=True)
    tap.sync_one("issues")


def test_gitlab_sync_releases():
    """Test sync_one() for gitlab sample."""
    tap = SampleTapGitlab(config=config, parse_env_config=True)
    tap.sync_one("releases")


# def test_gitlab_sync_all():
#     """Test sync_all() for gitlab sample."""
#     tap = SampleTapGitlab(config=SAMPLE_CONFIG)
#     tap.sync_all()
