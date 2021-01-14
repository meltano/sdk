"""Sample tap test for tap-gitlab."""

from tap_base.typehelpers import ArrayType, DateTimeType, PropertiesList, StringType
from typing import List
from tap_base import Tap, Stream
from tap_base.samples.sample_tap_gitlab.gitlab_rest_streams import (
    ProjectsStream,
    ReleasesStream,
    IssuesStream,
    CommitsStream,
)
from tap_base.samples.sample_tap_gitlab.gitlab_graphql_streams import (
    GraphQLCurrentUserStream,
)
from tap_base.samples.sample_tap_gitlab.gitlab_globals import PLUGIN_NAME


STREAM_TYPES = [
    ProjectsStream,
    ReleasesStream,
    IssuesStream,
    CommitsStream,
    GraphQLCurrentUserStream,
]


class SampleTapGitlab(Tap):
    """Sample tap for Gitlab."""

    name: str = PLUGIN_NAME
    config_jsonschema = PropertiesList(
        StringType("auth_token"),
        ArrayType("project_ids", StringType),
        DateTimeType("start_date"),
        StringType("api_url", optional=True),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]


# CLI Execution:

cli = SampleTapGitlab.cli
