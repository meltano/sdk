"""Sample tap stream test for tap-gitlab."""

from pathlib import Path
from typing import Any, Dict, List, Union

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

    def get_query_params(self) -> Union[List[Dict[str, Any]], Dict[str, Any]]:
        """Expose any needed config values into the URL parameterization process.

        If a list of dictionaries is returned, one call will be made for each item
        in the list.
        """
        result: List[URLArgMap] = []
        project_ids = self.get_config("project_ids")
        if isinstance(project_ids, str):
            id_list = project_ids.split(",")
        else:
            id_list = project_ids
        for project_id in id_list:
            result.append(
                {"project_id": project_id, "start_date": self.get_config("start_date")}
            )
        return result


class ProjectsStream(GitlabStream):
    name = "projects"
    url_suffix = "/projects/{project_id}?statistics=1"
    primary_keys = ["id"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "projects.json"


class ReleasesStream(GitlabStream):
    name = "releases"
    url_suffix = "/projects/{project_id}/releases"
    primary_keys = ["project_id", "commit_id", "tag_name"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "releases.json"


class IssuesStream(GitlabStream):
    name = "issues"
    url_suffix = "/projects/{project_id}/issues?scope=all&updated_after={start_date}"
    primary_keys = ["id"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "issues.json"


class CommitsStream(GitlabStream):
    name = "commits"
    url_suffix = (
        "/projects/{project_id}/repository/commits?since={start_date}&with_stats=true"
    )
    primary_keys = ["id"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "commits.json"
