"""Sample tap test for tap-gitlab."""

from datetime import datetime
from typing import List

from pydantic import BaseModel

from singer_sdk import Tap, Stream
from singer_sdk.plugin_base import BasePluginConfig
from singer_sdk.samples.sample_tap_gitlab.gitlab_rest_streams import (
    ProjectsStream,
    ReleasesStream,
    IssuesStream,
    CommitsStream,
    EpicsStream,
    # EpicIssuesStream,  # Temporarily skipped due to access denied error
)


STREAM_TYPES = [
    ProjectsStream,
    ReleasesStream,
    IssuesStream,
    CommitsStream,
    EpicsStream,
    # EpicIssuesStream,  # Temporarily skipped due to access denied error
]


class TapGitlabConfig(BasePluginConfig):
    """Configuration class for the GitLab API."""
    auth_token: str
    project_ids: List[str]
    group_ids: List[str]
    start_date: datetime

    class Config(BaseSettings.Config):
        env_prefix = "tap_gitlab_"


class SampleTapGitlab(Tap):
    """Sample tap for Gitlab."""

    name: str = "sample-tap-gitlab"
    config_model = TapGitlabConfig

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]


# CLI Execution:

cli = SampleTapGitlab.cli
