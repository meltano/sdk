"""Sample tap test for tap-gitlab."""

from singer_sdk.helpers.typing import (
    ArrayType,
    DateTimeType,
    PropertiesList,
    StringType,
)
from typing import List
from singer_sdk import Tap, Stream
from singer_sdk.samples.sample_tap_gitlab.gitlab_rest_streams import (
    ProjectsStream,
    ReleasesStream,
    IssuesStream,
    CommitsStream,
    EpicsStream,
    EpicIssuesStream,
)
from singer_sdk.samples.sample_tap_gitlab.gitlab_globals import PLUGIN_NAME


STREAM_TYPES = [
    ProjectsStream,
    ReleasesStream,
    IssuesStream,
    CommitsStream,
    EpicsStream,
    EpicIssuesStream,
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
        """Return a list of discovered  in order of execution.

        Streams with parent stream dependencies will be returned last, otherwise
        streams will be in alphabetical order.
        """
        return sorted(
            [stream_class(tap=self) for stream_class in STREAM_TYPES],
            key=lambda x: (len(x.parent_stream_types or []), x.name),
            reverse=True,
        )


# CLI Execution:

cli = SampleTapGitlab.cli
