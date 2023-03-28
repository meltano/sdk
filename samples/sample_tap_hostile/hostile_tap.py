"""A sample tap for testing SQL target property name transformations."""

from __future__ import annotations

from samples.sample_tap_hostile.hostile_streams import HostilePropertyNamesStream
from singer_sdk import Stream, Tap
from singer_sdk.typing import PropertiesList


class SampleTapHostile(Tap):
    """Sample tap for for testing SQL target property name transformations."""

    name: str = "sample-tap-hostile"
    config_jsonschema = PropertiesList().to_dict()

    def discover_streams(self) -> list[Stream]:
        """Return a list of discovered streams."""
        return [
            HostilePropertyNamesStream(tap=self),
        ]


if __name__ == "__main__":
    SampleTapHostile.cli()
