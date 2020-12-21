"""Sample tap test for tap-gitlab."""

import click

from tap_base.tap_base import TapBase
from tap_base.tests.sample_tap_gitlab.gitlab_rest_streams import (
    GitlabStream,
    ProjectsStream,
    ReleasesStream,
    IssuesStream,
    CommitsStream,
)
from tap_base.tests.sample_tap_gitlab.gitlab_graphql_streams import (
    GraphQLCurrentUserStream,
)

STREAM_TYPES = [
    # GitlabStream,
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

    def discover_catalog_streams(self) -> None:
        """Initialize self._streams with a dictionary of all streams."""
        # Add REST and GraphQL Streams
        for stream_class in STREAM_TYPES:
            stream = stream_class(config=self._config, state={})
            self._streams[stream.name] = stream


# CLI Execution:
# TODO: Move entirely to base class (https://gitlab.com/meltano/tap-base/-/issues/8)


@click.option("--version", is_flag=True)
@click.option("--discover", is_flag=True)
@click.option("--config")
@click.option("--catalog")
@click.command()
def cli(
    discover: bool = False,
    config: str = None,
    catalog: str = None,
    version: bool = False,
):
    """Handle CLI Execution."""
    SampleTapGitlab.cli(
        version=version, discover=discover, config=config, catalog=catalog
    )
