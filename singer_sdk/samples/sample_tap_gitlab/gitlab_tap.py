"""Sample tap test for tap-gitlab."""

from singer_sdk.typing import (
    ArrayType,
    DateTimeType,
    Property,
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

    name: str = "sample-tap-gitlab"
    config_jsonschema = PropertiesList(
        Property("auth_token", StringType, required=True),
        Property("project_ids", ArrayType(StringType), required=True),
        Property("group_ids", ArrayType(StringType), required=True),
        Property("start_date", DateTimeType, required=True),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]
