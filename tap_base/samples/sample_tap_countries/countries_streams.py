"""Sample tap stream test for tap-countries.

This uses a free "Countries API" which does not require authentication.

See the online explorer and query builder here:
  - https://countries.trevorblades.com/
"""

import abc
from pathlib import Path

from tap_base.authenticators import SimpleAuthenticator
from tap_base.streams.graphql import GraphQLStreamBase

from tap_base.samples.sample_tap_countries.countries_globals import PLUGIN_NAME

SCHEMAS_DIR = Path("./tap_base/samples/sample_tap_countries/schemas")


class CountriesAPIStreamBase(GraphQLStreamBase, metaclass=abc.ABCMeta):
    """Sample tap test for countries."""

    tap_name = PLUGIN_NAME
    site_url_base = "https://countries.trevorblades.com/"
    authenticator = SimpleAuthenticator(auth_header={})  # No auth needed.


class CountriesStream(CountriesAPIStreamBase):

    name = "countries"
    schema_filepath = "./tap_base/samples/sample_tap_countries/schemas/countries.json"
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
