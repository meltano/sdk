"""Sample tap test for tap-gitlab."""

from typing import List
from tap_base import TapBase, TapStreamBase
from tap_base.tests.sample_tap_gitlab.gitlab_rest_streams import (
    ProjectsStream,
    ReleasesStream,
    IssuesStream,
    CommitsStream,
)
from tap_base.tests.sample_tap_gitlab.gitlab_graphql_streams import (
    GraphQLCurrentUserStream,
)

STREAM_TYPES = [
    ProjectsStream,
    ReleasesStream,
    IssuesStream,
    CommitsStream,
    GraphQLCurrentUserStream,
]


class SampleTapGitlab(TapBase):
    """Sample tap for Gitlab."""

    name: str = "sample-tap-gitlab"
    accepted_config_keys = ["auth_token", "project_ids", "start_date"]
    required_config_options = [["auth_token", "project_ids", "start_date"]]

    def discover_streams(self) -> List[TapStreamBase]:
        """Return a list of discovered streams."""
        return [
            stream_class(config=self._config, state={}) for stream_class in STREAM_TYPES
        ]


# CLI Execution:

cli = SampleTapGitlab.build_cli()
