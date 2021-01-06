"""Sample tap stream test for tap-countries.

This uses a free "Countries API" which does not require authentication.

See the online explorer and query builder here:
  - https://countries.trevorblades.com/
"""

import abc
from pathlib import Path

from tap_base.authenticators import SimpleAuthenticator
from tap_base.streams.graphql import GraphQLStream

SCHEMAS_DIR = Path("./tap_base/samples/sample_tap_countries/schemas")


class CountriesAPIStream(GraphQLStream, metaclass=abc.ABCMeta):
    """Sample tap test for countries."""

    url_base = "https://countries.trevorblades.com/"

    @property
    def authenticator(self):
        """Return the authenticator for this stream."""
        return SimpleAuthenticator(stream=self, http_headers={})  # No auth needed.


class CountriesStream(CountriesAPIStream):

    name = "countries"
    schema_filepath = "./tap_base/samples/sample_tap_countries/schemas/countries.json"
    query = """
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


class ContinentsStream(CountriesAPIStream):

    name = "continents"
    schema_filepath = SCHEMAS_DIR / "continents.json"
    query = """
        continents {
            code
            name
        }
        """
