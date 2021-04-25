"""Sample tap stream test for tap-google-analytics."""

from pathlib import Path
from typing import Iterable, Optional, Any

import pendulum

from singer_sdk.streams import RESTStream
from singer_sdk.authenticators import OAuthJWTAuthenticator

GOOGLE_OAUTH_ENDPOINT = "https://oauth2.googleapis.com/token"
GA_OAUTH_SCOPES = "https://www.googleapis.com/auth/analytics.readonly"
SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class GoogleJWTAuthenticator(OAuthJWTAuthenticator):
    """Class responsible for Google Auth via JWT and OAuth."""

    @property
    def client_id(self) -> str:
        """Override since Google auth uses email, not numeric client ID."""
        return self.config["client_email"]


class SampleGoogleAnalyticsStream(RESTStream):
    """Sample tap test for google-analytics."""

    url_base = "https://analyticsreporting.googleapis.com/v4"
    path = "/reports:batchGet"
    rest_method = "POST"

    @property
    def authenticator(self) -> GoogleJWTAuthenticator:
        """Return authenticator for Google Analytics."""
        return GoogleJWTAuthenticator(
            stream=self,
            auth_endpoint=GOOGLE_OAUTH_ENDPOINT,
            oauth_scopes=GA_OAUTH_SCOPES,
        )

    def prepare_request_payload(
        self, partition: Optional[dict], next_page_token: Optional[Any] = None
    ) -> Optional[dict]:
        """Prepare the data payload for the REST API request."""
        # params = self.get_url_params(partition, next_page_token)
        request_def = {
            "viewId": self.config["view_id"],
            "metrics": [{"expression": m} for m in self.metrics],
            "dimensions": [{"name": d} for d in self.dimensions],
            # "orderBys": [
            #     {"fieldName": "ga:sessions", "sortOrder": "DESCENDING"},
            #     {"fieldName": "ga:pageviews", "sortOrder": "DESCENDING"},
            # ],
        }
        if self.config.get("start_date"):
            request_def["dateRanges"] = [
                {
                    "startDate": self.config.get("start_date"),
                    "endDate": pendulum.datetime.now(),
                }
            ]
        return {"reportRequests": [request_def]}

    def parse_response(self, response) -> Iterable[dict]:
        """Parse Google Analytics API response into individual records."""
        self.logger.info(
            f"Received raw Google Analytics query response: {response.json()}"
        )
        report_data = response.json().get("reports", [{}])[0].get("data")
        if not report_data:
            self.logger.info(
                f"Received empty Google Analytics query response: {response.json()}"
            )
        for total in report_data["totals"]:
            yield {"totals": total["values"]}


class GASimpleSampleStream(SampleGoogleAnalyticsStream):
    """A super simple sample report."""

    name = "simple_sample"
    schema_filepath = SCHEMAS_DIR / "simple-sample.json"

    dimensions = ["ga:date"]
    metrics = ["ga:users", "ga:sessions"]
