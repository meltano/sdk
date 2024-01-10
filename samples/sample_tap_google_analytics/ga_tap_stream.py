"""Sample tap stream test for tap-google-analytics."""

from __future__ import annotations

import datetime
import typing as t

from singer_sdk.authenticators import OAuthJWTAuthenticator
from singer_sdk.helpers._compat import importlib_resources
from singer_sdk.streams import RESTStream

GOOGLE_OAUTH_ENDPOINT = "https://oauth2.googleapis.com/token"
GA_OAUTH_SCOPES = "https://www.googleapis.com/auth/analytics.readonly"
SCHEMAS_DIR = importlib_resources.files(__package__) / "schemas"


class GoogleJWTAuthenticator(OAuthJWTAuthenticator):
    """Class responsible for Google Auth via JWT and OAuth."""

    @property
    def client_id(self) -> str:
        """Override since Google auth uses email, not numeric client ID."""
        return t.cast(str, self.config["client_email"])


class SampleGoogleAnalyticsStream(RESTStream):
    """Sample tap test for google-analytics."""

    url_base = "https://analyticsreporting.googleapis.com/v4"
    path = "/reports:batchGet"
    rest_method = "POST"

    # Child class overrides:
    dimensions: tuple[str] = ()
    metrics: tuple[str] = ()

    @property
    def authenticator(self) -> GoogleJWTAuthenticator:
        """Return authenticator for Google Analytics."""
        return GoogleJWTAuthenticator(
            stream=self,
            auth_endpoint=GOOGLE_OAUTH_ENDPOINT,
            oauth_scopes=GA_OAUTH_SCOPES,
        )

    def prepare_request_payload(
        self,
        context: dict | None,  # noqa: ARG002
        next_page_token: t.Any | None,  # noqa: ARG002
    ) -> dict | None:
        """Prepare the data payload for the REST API request."""
        request_def = {
            "viewId": self.config["view_id"],
            "metrics": [{"expression": m} for m in self.metrics],
            "dimensions": [{"name": d} for d in self.dimensions],
        }
        if self.config.get("start_date"):
            request_def["dateRanges"] = [
                {
                    "startDate": self.config.get("start_date"),
                    "endDate": datetime.datetime.now(datetime.timezone.utc),
                },
            ]
        return {"reportRequests": [request_def]}

    def parse_response(self, response) -> t.Iterable[dict]:
        """Parse Google Analytics API response into individual records."""
        self.logger.info(
            "Received raw Google Analytics query response: %s",
            response.json(),
        )
        report_data = response.json().get("reports", [{}])[0].get("data")
        if not report_data:
            self.logger.info(
                "Received empty Google Analytics query response: %s",
                response.json(),
            )
        for total in report_data["totals"]:
            yield {"totals": total["values"]}


class GASimpleSampleStream(SampleGoogleAnalyticsStream):
    """A super simple sample report."""

    name = "simple_sample"
    schema_filepath = SCHEMAS_DIR / "simple-sample.json"

    dimensions = ("ga:date",)
    metrics = ("ga:users", "ga:sessions")
