"""Sample tap stream test for tap-gitlab.

# See the interactive GraphQL query builder for GitLab data models here:
#  - https://gitlab.com/-/graphql-explorer
"""

from pathlib import Path
from jinja2 import Template

from singer_sdk.authenticators import SimpleAuthenticator
from singer_sdk.streams import GraphQLStream


SITE_URL = "https://gitlab.com/graphql"

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class GitlabGraphQLStream(GraphQLStream):
    """Sample tap test for gitlab."""

    url_base = SITE_URL

    @property
    def http_headers(self) -> dict:
        """Return headers dict to be used for HTTP requests."""
        result = super().http_headers
        result["Authorization"] = f"token {self.config.get('auth_token')}"
        return result

    @property
    def authenticator(self) -> SimpleAuthenticator:
        """Return an authenticator for GraphQL API requests."""
        return SimpleAuthenticator(self)


class GraphQLCurrentUserStream(GitlabGraphQLStream):
    """Gitlab Current User stream."""

    name = "currentuser"
    primary_keys = ["id"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "currentuser.json"
    query = """
        currentUser {
            name
        }
        """


class GraphQLProjectsStream(GitlabGraphQLStream):
    """Gitlab Projects stream."""

    name = "projects"
    primary_keys = ["id"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "projects-graphql.json"
    query = Template(
        """
        project(fullPath: $project_id) {
            name
        }
        """
    ).render()
