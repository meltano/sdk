"""Sample tap test for tap-countries.

This uses a free "Countries API" which does not require authentication.

See the online explorer and query builder here:
  - https://countries.trevorblades.com/
"""

from __future__ import annotations

import sys
import typing as t

from singer_sdk import Tap
from singer_sdk.contrib.msgspec import MsgSpecWriter
from tap_countries.streams import ContinentsStream, CountriesStream

if sys.version_info >= (3, 12):
    from typing import override  # noqa: ICN003
else:
    from typing_extensions import override


if t.TYPE_CHECKING:
    from singer_sdk import Stream


class TapCountries(Tap):
    """Sample tap for Countries GraphQL API."""

    name: str = "tap-countries"
    package_name: str = "meltano-tap-countries"

    message_writer_class = MsgSpecWriter

    @override
    def discover_streams(self) -> list[Stream]:
        """Return a list of discovered streams."""
        return [
            CountriesStream(tap=self),
            ContinentsStream(tap=self),
        ]


if __name__ == "__main__":
    TapCountries.cli()
