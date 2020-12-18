"""Sample tap stream test for tap-gitlab.

# See the interactive GraphQL query builder for GitLab data models here:
#  - https://gitlab.com/-/graphql-explorer
"""

from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Union

from tap_base.streams.rest import RESTStreamBase

from jinja2 import Template

SITE_URL = "https://gitlab.com/graphql"

URLArgMap = Dict[str, Union[str, bool, int, datetime]]

SCHEMAS_DIR = Path("./tap_base/tests/sample_tap_gitlab/schemas")


class GitlabGraphQLStreamBase(RESTStreamBase):
    """Sample tap test for gitlab."""

    site_url_base = SITE_URL

    def get_auth_header(self) -> Dict[str, Any]:
        """Return an authorization header for GraphQL API requests."""
        return {"Authorization": f"token {self.get_config('auth_token')}"}

    def post_process(self, row: dict) -> dict:
        """Transform raw data from HTTP GET into the expected property values."""
        return row

    # def get_url(self, url_suffix: str = None, extra_url_args: URLArgMap = None) -> str:
    #     replacement_map = {
    #         # TODO: Handle multiple projects:
    #         "project_id": self.get_config("project_ids")[0],
    #         "start_date": self.get_config("start_date"),
    #     }
    #     if extra_url_args:
    #         replacement_map.update(extra_url_args)
    #     return super().get_url(url_suffix=url_suffix, extra_url_args=replacement_map)


class GraphQLCurrentUserStream(GitlabGraphQLStreamBase):

    name = "currentuser"
    primary_keys = ["id"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "currentuser.json"
    graphql_query = """
        currentUser {
            name
        }
        """


class GraphQLProjectsStream(GitlabGraphQLStreamBase):

    name = "projects"
    primary_keys = ["id"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "projects-graphql.json"
    graphql_query = Template(
        """
        project(fullPath: "{{ project }}") {
            name
        }
        """
    ).render()
