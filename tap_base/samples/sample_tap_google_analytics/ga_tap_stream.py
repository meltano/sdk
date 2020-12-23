"""Sample tap stream test for tap-google-analytics."""

from pathlib import Path
from typing import Dict, Iterable, Optional

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
    # site_url_base = "https://analyticsreporting.googleapis.com/v4/reports:batchGet"
    site_url_base = "https://analyticsreporting.googleapis.com/v4/reports"
    url_suffix = ""

    def get_authenticator(self) -> OAuthJWTAuthenticator:
        return GoogleJWTAuthenticator(
            config=self._config,
            auth_endpoint=GOOGLE_OAUTH_ENDPOINT,
            oauth_scopes=GA_OAUTH_SCOPES,
        )


class GASimpleSampleStream(SampleGoogleAnalyticsStream):
    """A super simple sample report."""

    name = "simple_sample"
    schema_filepath = SCHEMAS_DIR / "simple-sample.json"

    dimensions = ["ga:date"]
    metrics = ["ga:users", "ga:sessions"]
