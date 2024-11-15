"""Sample tap test for tap-gitlab."""

from __future__ import annotations

from samples.sample_tap_gitlab.gitlab_rest_streams import (
    CommitsStream,
    EpicIssuesStream,
    EpicsStream,
    IssuesStream,
    ProjectsStream,
    ReleasesStream,
)
from singer_sdk import Stream, Tap
from singer_sdk.typing import (
    ArrayType,
    DateTimeType,
    PropertiesList,
    Property,
    StringType,
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
        Property(
            "auth_token", StringType, required=True, secret=True, title="Auth Token"
        ),
        Property(
            "project_ids", ArrayType(StringType), required=True, title="Project IDs"
        ),
        Property("group_ids", ArrayType(StringType), required=True, title="Group IDs"),
        Property("start_date", DateTimeType, required=True, title="Start Date"),
    ).to_dict()

    def discover_streams(self) -> list[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]
