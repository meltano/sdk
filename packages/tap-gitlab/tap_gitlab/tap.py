"""Sample tap test for tap-gitlab."""

from __future__ import annotations

import typing as t

from singer_sdk import Tap
from singer_sdk.typing import (
    ArrayType,
    DateTimeType,
    PropertiesList,
    Property,
    StringType,
)
from tap_gitlab.streams import (
    CommitsStream,
    EpicIssuesStream,
    EpicsStream,
    IssuesStream,
    ProjectsStream,
    ReleasesStream,
)

if t.TYPE_CHECKING:
    from singer_sdk import Stream

DEFAULT_URL_BASE = "https://gitlab.com/api/v4"
STREAM_TYPES = [
    ProjectsStream,
    ReleasesStream,
    IssuesStream,
    CommitsStream,
    EpicsStream,
    EpicIssuesStream,
]


class TapGitlab(Tap):
    """Sample tap for Gitlab."""

    name: str = "tap-gitlab"
    config_jsonschema = PropertiesList(
        Property(
            "auth_token",
            StringType,
            required=True,
            secret=True,
            title="Auth Token",
        ),
        Property(
            "project_ids",
            ArrayType(StringType),
            nullable=False,
            default=[],
            title="Project IDs",
        ),
        Property(
            "group_ids",
            ArrayType(StringType),
            nullable=False,
            default=[],
            title="Group IDs",
        ),
        Property(
            "start_date",
            DateTimeType,
            required=True,
            title="Start Date",
        ),
        Property(
            "url_base",
            StringType,
            required=False,
            default=DEFAULT_URL_BASE,
            title="GitLab API URL",
        ),
    ).to_dict()

    def discover_streams(self) -> list[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]
