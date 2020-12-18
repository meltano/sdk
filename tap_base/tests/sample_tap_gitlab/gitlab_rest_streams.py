"""Sample tap stream test for tap-gitlab."""

from pathlib import Path
from typing import Any, Dict

from tap_base.streams.rest import RESTStreamBase, URLArgMap

SCHEMAS_DIR = Path("./tap_base/tests/sample_tap_gitlab/schemas")


class GitlabStream(RESTStreamBase):
    """Sample tap test for gitlab."""

    tap_name = "sample-tap-gitlab"
    site_url_base = "https://gitlab.com/api/v4"

    def get_auth_header(self) -> Dict[str, Any]:
        """Return an authorization header for REST API requests."""
        result = {"Private-Token": self.get_config("auth_token")}
        if self.get_config("user_agent"):
            result["User-Agent"] = self.get_config("user_agent")
        return result

    def post_process(self, row: dict) -> dict:
        """Transform raw data from HTTP GET into the expected property values."""
        return row

    def get_url(self, url_suffix: str = None, extra_url_args: URLArgMap = None) -> str:
        # TODO: Handle this automatically in the framework's base class,
        #       apply template logic to fill placeholders.
        replacement_map = {
            # TODO: Handle multiple projects:
            "project_id": self.get_config("project_ids")[0],
            "start_date": self.get_config("start_date"),
        }
        if extra_url_args:
            replacement_map.update(extra_url_args)
        return super().get_url(url_suffix=url_suffix, extra_url_args=replacement_map)


class ProjectsStream(GitlabStream):
    stream_name = "projects"
    url_suffix = "/projects/{project_id}?statistics=1"
    primary_keys = ["id"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "projects.json"


class ReleasesStream(GitlabStream):
    stream_name = "releases"
    url_suffix = "/projects/{project_id}/releases"
    primary_keys = ["project_id", "commit_id", "tag_name"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "releases.json"


class IssuesStream(GitlabStream):
    stream_name = "issues"
    url_suffix = "/projects/{project_id}/issues?scope=all&updated_after={start_date}"
    primary_keys = ["id"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "issues.json"


class CommitsStream(GitlabStream):
    stream_name = "commits"
    url_suffix = (
        "/projects/{project_id}/repository/commits?since={start_date}&with_stats=true"
    )
    primary_keys = ["id"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "commits.json"
