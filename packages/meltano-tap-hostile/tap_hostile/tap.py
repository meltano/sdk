"""A sample tap for testing SQL target property name transformations."""

from __future__ import annotations

import sys
import typing as t

from singer_sdk import Tap
from singer_sdk.typing import PropertiesList
from tap_hostile.streams import HostilePropertyNamesStream

if sys.version_info >= (3, 12):
    from typing import override  # noqa: ICN003
else:
    from typing_extensions import override


if t.TYPE_CHECKING:
    from singer_sdk import Stream


class TapHostile(Tap):
    """Sample tap for for testing SQL target property name transformations."""

    name: str = "tap-hostile"
    package_name: str = "meltano-tap-hostile"
    config_jsonschema = PropertiesList().to_dict()

    @override
    def discover_streams(self) -> list[Stream]:
        """Return a list of discovered streams."""
        return [
            HostilePropertyNamesStream(tap=self),
        ]


if __name__ == "__main__":
    TapHostile.cli()
