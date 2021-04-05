"""Sample tap stream test for tap-gitlab."""

import requests

from pathlib import Path
from typing import Any, Dict, List, cast, Optional

from singer_sdk.typing import (
    ArrayType,
    DateTimeType,
    IntegerType,
    Property,
    PropertiesList,
    StringType,
)
from singer_sdk.helpers._state import get_writeable_state_dict
from singer_sdk.authenticators import SimpleAuthenticator
from singer_sdk.streams.rest import RESTStream

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")

DEFAULT_URL_BASE = "https://gitlab.com/api/v4"


class GitlabStream(RESTStream):
    """Sample tap test for gitlab."""

    @property
    def url_base(self) -> str:
        """Return the base GitLab URL."""
        return self.config.get("api_url", DEFAULT_URL_BASE)

    @property
    def authenticator(self) -> SimpleAuthenticator:
        """Return an authenticator for REST API requests."""
        return SimpleAuthenticator(
            stream=self, auth_headers={"Private-Token": self.config.get("auth_token")}
        )

    def get_url_params(
        self, partition: Optional[dict], next_page_token: Optional[Any] = None
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        return {
            "start_date": self.get_starting_timestamp(partition),
            "page": next_page_token or 1,
        }

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any] = None
    ) -> Optional[Any]:
        """Return token for identifying next page or None if not applicable."""
        next_page_token = response.headers.get("X-Next-Page", None)
        if next_page_token:
            self.logger.info(f"Next page token retrieved: {next_page_token}")
        return next_page_token


class ProjectBasedStream(GitlabStream):
    """Base class for streams that are keys based on project ID."""

    @property
    def partitions(self) -> List[dict]:
        """Return a list of partition key dicts (if applicable), otherwise None."""
        if "{project_id}" in self.path:
            return [
                {"project_id": id} for id in cast(list, self.config.get("project_ids"))
            ]
        if "{group_id}" in self.path:
            if "group_ids" not in self.config:
                raise ValueError(
                    f"Missing `group_ids` setting which is required for the "
                    f"'{self.name}' stream."
                )
            return [{"group_id": id} for id in cast(list, self.config.get("group_ids"))]
        raise ValueError(
            "Could not detect partition type for Gitlab stream "
            f"'{self.name}' ({self.path}). "
            "Expected a URL path containing '{project_id}' or '{group_id}'. "
        )


class ProjectsStream(ProjectBasedStream):
    """Gitlab Projects stream."""

    name = "projects"
    path = "/projects/{project_id}?statistics=1"
    primary_keys = ["id"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "projects.json"


class ReleasesStream(ProjectBasedStream):
    """Gitlab Releases stream."""

    name = "releases"
    path = "/projects/{project_id}/releases"
    primary_keys = ["project_id", "commit_id", "tag_name"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "releases.json"


class IssuesStream(ProjectBasedStream):
    """Gitlab Issues stream."""

    name = "issues"
    path = "/projects/{project_id}/issues?scope=all&updated_after={start_date}"
    primary_keys = ["id"]
    replication_key = "updated_at"  # TODO: Validate this is valid for replication
    schema_filepath = SCHEMAS_DIR / "issues.json"


class CommitsStream(ProjectBasedStream):
    """Gitlab Commits stream."""

    name = "commits"
    path = (
        "/projects/{project_id}/repository/commits?since={start_date}&with_stats=true"
    )
    primary_keys = ["id"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "commits.json"


class EpicsStream(ProjectBasedStream):
    """Gitlab Epics stream.

    This class shows an example of inline `schema` declaration rather than referencing
    a raw json input file.
    """

    name = "epics"
    path = "/groups/{group_id}/epics?updated_after={start_date}"
    primary_keys = ["id"]
    replication_key = None
    schema = PropertiesList(
        Property("id", IntegerType, required=True),
        Property("iid", IntegerType, required=True),
        Property("group_id", IntegerType, required=True),
        Property("parent_id", IntegerType),
        Property("title", StringType),
        Property("description", StringType),
        Property("state", StringType),
        Property("author_id", IntegerType),
        Property("start_date", DateTimeType),
        Property("end_date", DateTimeType),
        Property("due_date", DateTimeType),
        Property("created_at", DateTimeType),
        Property("updated_at", DateTimeType),
        Property("labels", ArrayType(StringType)),
        Property("upvotes", IntegerType),
        Property("downvotes", IntegerType),
    ).to_dict()

    # schema_filepath = SCHEMAS_DIR / "epics.json"

    def post_process(self, row: dict, partition: Optional[dict] = None) -> dict:
        """Perform post processing, including queuing up any child stream types."""
        # Ensure child state record(s) are created
        _ = get_writeable_state_dict(
            self.tap_state,
            "epic_issues",
            partition={
                "group_id": row["group_id"],
                "epic_id": row["id"],
            },
        )
        return super().post_process(row, partition)


class EpicIssuesStream(GitlabStream):
    """EpicIssues stream class.

    NOTE: This should only be run after epics have been synced, since epic streams
          have a dependency on the state generated by epics.
    """

    name = "epic_issues"
    path = "/groups/{group_id}/epics/{epic_id}/issues"
    primary_keys = ["id"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "epic_issues.json"
    parent_stream_types = [EpicsStream]  # Stream should wait for parents to complete.

    def get_url_params(
        self, partition: Optional[dict], next_page_token: Optional[Any] = None
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in parameterization."""
        result = super().get_url_params(partition)
        if "epic_id" not in partition:
            raise ValueError("Cannot sync epic issues without already known epic IDs.")
        return result
