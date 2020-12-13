"""Sample tap stream test for tap-countries."""

import abc
from pathlib import Path
from typing import Any, Dict

from tap_base.streams.graphql import GraphQLStreamBase

SCHEMAS_DIR = Path("./tap_base/tests/sample_tap_countries/schemas")


class CountriesAPIStreamBase(GraphQLStreamBase, metaclass=abc.ABCMeta):
    """Sample tap test for countries."""

    site_url_base = "https://countries.trevorblades.com/"

    def get_auth_header(self) -> Dict[str, Any]:
        """Return an authorization header for GraphQL request."""
        return {}  # No auth required

    def post_process(self, row: dict) -> dict:
        """Transform raw data from HTTP GET into the expected property values."""
        return row


class CountriesStream(CountriesAPIStreamBase):

    name = "countries"
    schema_filepath = "./tap_base/tests/sample_tap_countries/schemas/countries.json"
    graphql_query = """
        countries {
            code
            name
            native
            phone
            continent {
                code
                name
            }
            capital
            currency
            languages {
                code
                name
            }
            emoji
        }
        """


class ContinentsStream(CountriesAPIStreamBase):

    name = "continents"
    schema_filepath = SCHEMAS_DIR / "continents.json"
    graphql_query = """
        continents {
            code
            name
        }
        """
