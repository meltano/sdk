"""Sample tap stream test for tap-google-analytics."""

from pathlib import Path

import requests

from tap_base.streams import RESTStreamBase
from tap_base.authenticators import OAuthJWTAuthenticator

from tap_base.samples.sample_tap_google_analytics.ga_globals import PLUGIN_NAME

GOOGLE_OAUTH_ENDPOINT = "https://oauth2.googleapis.com/token"
GA_OAUTH_SCOPES = "https://www.googleapis.com/auth/analytics.readonly"
SCHEMAS_DIR = Path("./tap_base/samples/sample_tap_google_analytics/schemas")


class GoogleJWTAuthenticator(OAuthJWTAuthenticator):
    """Class responsible for Google Auth."""


class SampleGoogleAnalyticsStream(RESTStreamBase):
    """Sample tap test for google-analytics."""

    tap_name = PLUGIN_NAME
    rest_method = "POST"
    site_url_base = "https://analyticsreporting.googleapis.com/v4"
    url_suffix = "/reports:batchGet"

    def get_authenticator(self) -> OAuthJWTAuthenticator:
        return GoogleJWTAuthenticator(
            config=self._config,
            auth_endpoint=GOOGLE_OAUTH_ENDPOINT,
            oauth_scopes=GA_OAUTH_SCOPES,
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
                    "metrics": [{"expression": m} for m in self.metrics],
                    "dimensions": [{"name": d} for d in self.dimensions],
                    # "dateRanges": [
                    #     {
                    #         "startDate": report_date_string,
                    #         "endDate": report_date_string
                    #     }
                    # ],
                    # "orderBys": [
                    #     {"fieldName": "ga:sessions", "sortOrder": "DESCENDING"},
                    #     {"fieldName": "ga:pageviews", "sortOrder": "DESCENDING"},
                    # ],
                }
            ]
        }
        self.logger.info(f"Attempting query:\n{json_msg}")
        return super().prepare_request(
            url=url, params=params, method=method, json=json_msg
        )


class GASimpleSampleStream(SampleGoogleAnalyticsStream):
    """A super simple sample report."""

    name = "simple_sample"
    schema_filepath = SCHEMAS_DIR / "simple-sample.json"

    dimensions = ["ga:date"]
    metrics = ["ga:users", "ga:sessions"]
