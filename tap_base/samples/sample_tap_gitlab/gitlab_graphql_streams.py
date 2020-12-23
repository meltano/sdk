"""Sample tap stream test for tap-gitlab.

# See the interactive GraphQL query builder for GitLab data models here:
#  - https://gitlab.com/-/graphql-explorer
"""

from datetime import datetime
from pathlib import Path
from typing import Dict, Union

from jinja2 import Template
import requests

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

    def prepare_request(
        self, url, params=None, method="POST", json=None
    ) -> requests.PreparedRequest:
        """Prepare GraphQL API request."""
        if method != "POST":
            raise ValueError("Argument 'method' must be 'POST' for GraphQL streams.")
        if not self.dimensions:
            raise ValueError("Missing value for 'dimensions'.")
        if not self.metrics:
            raise ValueError("Missing value for 'metrics'.")
        json_msg = {
            "reportRequests": [
                {
                    "viewId": self.get_config("view_id"),
                    "dimensions": self.dimensions,
                    "metrics": self.metrics,
                    # "orderBys": [
                    #     {"fieldName": "ga:sessions", "sortOrder": "DESCENDING"},
                    #     {"fieldName": "ga:pageviews", "sortOrder": "DESCENDING"},
                    # ],
                }
            ]
        }
        self.logger.info(f"Attempting query:\n{json_msg}")
        return super().prepare_request(
            url=url, params=params, method="batchGet", json=json_msg
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
