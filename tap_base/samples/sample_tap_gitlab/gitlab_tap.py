"""Sample tap test for tap-gitlab."""

from typing import List, Type
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
from tap_base.samples.sample_tap_gitlab.gitlab_globals import (
    PLUGIN_NAME,
    ACCEPTED_CONFIG_OPTIONS,
    REQUIRED_CONFIG_SETS,
)


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
    accepted_config_keys = ACCEPTED_CONFIG_OPTIONS
    required_config_options = REQUIRED_CONFIG_SETS

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]

    @classmethod
    def get_stream_class(cls, stream_name: str) -> Type[Stream]:
        stream_type_matches = [t for t in STREAM_TYPES if t.name == stream_name]
        if len(stream_type_matches) == 1:
            return stream_type_matches[0]
        return super().get_stream_class(stream_name)


# CLI Execution:

cli = SampleTapGitlab.cli
