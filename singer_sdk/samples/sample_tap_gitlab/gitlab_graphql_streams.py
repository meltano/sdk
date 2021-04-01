"""Sample tap stream test for tap-gitlab.

# See the interactive GraphQL query builder for GitLab data models here:
#  - https://gitlab.com/-/graphql-explorer
"""

from pathlib import Path

from singer_sdk.streams import GraphQLStream


SITE_URL = "https://gitlab.com/graphql"

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class GitlabGraphQLStream(GraphQLStream):
    """Sample tap test for gitlab."""

    url_base = SITE_URL

    @property
    def http_headers(self) -> dict:
        """Return headers dict to be used for HTTP requests.

        Note: This sample implementation bypasses the SimpleAuthenticator class and
        simply returns the http_headers directly, with the auth_token.
        """
        return {"Authorization": f"token {self.config.get('auth_token')}"}


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

    @property
    def query(self) -> str:
        """Return dynamic GraphQL query."""
        return f"project(fullPath: {self.config('project_id')}" " { name }"
