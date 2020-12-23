"""Sample tap stream test for tap-gitlab.

# See the interactive GraphQL query builder for GitLab data models here:
#  - https://gitlab.com/-/graphql-explorer
"""

from datetime import datetime
from pathlib import Path
from typing import Dict, Union

from jinja2 import Template

from tap_base.authenticators import SimpleAuthenticator
from tap_base.streams.rest import RESTStreamBase


SITE_URL = "https://gitlab.com/graphql"

URLArgMap = Dict[str, Union[str, bool, int, datetime]]

SCHEMAS_DIR = Path("./tap_base/samples/sample_tap_gitlab/schemas")


class GitlabGraphQLStreamBase(RESTStreamBase):
    """Sample tap test for gitlab."""

    site_url_base = SITE_URL

    def get_authenticator(self) -> SimpleAuthenticator:
        """Return an authenticator for GraphQL API requests."""
        return SimpleAuthenticator(
            auth_header={"Authorization": f"token {self.get_config('auth_token')}"}
        )


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
