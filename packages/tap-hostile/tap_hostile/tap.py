"""A sample tap for testing SQL target property name transformations."""

from __future__ import annotations

import typing as t

from singer_sdk import Tap
from singer_sdk.typing import PropertiesList
from tap_hostile.streams import HostilePropertyNamesStream

if t.TYPE_CHECKING:
    from singer_sdk import Stream


class TapHostile(Tap):
    """Sample tap for for testing SQL target property name transformations."""

    name: str = "tap-hostile"
    config_jsonschema = PropertiesList().to_dict()

    def discover_streams(self) -> list[Stream]:
        """Return a list of discovered streams."""
        return [
            HostilePropertyNamesStream(tap=self),
        ]


if __name__ == "__main__":
    TapHostile.cli()
