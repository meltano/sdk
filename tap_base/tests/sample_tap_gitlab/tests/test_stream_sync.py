"""Test sample sync."""

import json
from pathlib import Path

from tap_base.tests.sample_tap_gitlab.gitlab_tap import SampleTapGitlab

COUNTER = 0


CONFIG_FILE = "tap_base/tests/sample_tap_gitlab/tests/.secrets/tap-gitlab.json"
SAMPLE_CONFIG = json.loads(Path(CONFIG_FILE).read_text())
SAMPLE_CONFIG_BAD = {"not": "correct"}


def test_gitlab_sync_projects():
    """Test sync_one() for gitlab sample."""
    tap = SampleTapGitlab(config=SAMPLE_CONFIG)
    tap.sync_one("projects")


def test_gitlab_sync_commits():
    """Test sync_one() for gitlab sample."""
    tap = SampleTapGitlab(config=SAMPLE_CONFIG)
    tap.sync_one("commits")


def test_gitlab_sync_issues():
    """Test sync_one() for gitlab sample."""
    tap = SampleTapGitlab(config=SAMPLE_CONFIG)
    tap.sync_one("issues")


def test_gitlab_sync_releases():
    """Test sync_one() for gitlab sample."""
    tap = SampleTapGitlab(config=SAMPLE_CONFIG)
    tap.sync_one("releases")


# def test_gitlab_sync_all():
#     """Test sync_all() for gitlab sample."""
#     tap = SampleTapGitlab(config=SAMPLE_CONFIG)
#     tap.sync_all()
