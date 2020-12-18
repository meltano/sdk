"""Sample tap stream test for tap-gitlab."""

from datetime import datetime
from typing import Any, Dict, Union

from tap_base.streams.rest import RESTStreamBase, URLArgMap

SITE_URL = "https://gitlab.com/api/v4"


class GitlabStream(RESTStreamBase):
    """Sample tap test for gitlab."""

    site_url_base = SITE_URL

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
        replacement_map = {
            # TODO: Handle multiple projects:
            "project_id": self.get_config("project_ids")[0],
            "start_date": self.get_config("start_date"),
        }
        if extra_url_args:
            replacement_map.update(extra_url_args)
        return super().get_url(url_suffix=url_suffix, extra_url_args=replacement_map)
