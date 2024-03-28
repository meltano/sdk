"""Test plugin config functions."""

from __future__ import annotations

import typing as t

from singer_sdk.tap_base import Tap
from singer_sdk.typing import BooleanType, PropertiesList, Property

if t.TYPE_CHECKING:
    from singer_sdk.streams.core import Stream

SAMPLE_CONFIG: dict[str, t.Any] = {}


class TapConfigTest(Tap):
    """Tap class for use in testing config operations."""

    name = "tap-config-test"
    config_jsonschema = PropertiesList(
        Property("default_true", BooleanType, default=True),
        Property("default_false", BooleanType, default=False),
    ).to_dict()

    def discover_streams(self) -> list[Stream]:
        """Noop."""
        return []


def test_tap_config_defaults():
    """Run standard tap tests from the SDK."""
    tap = TapConfigTest(config=SAMPLE_CONFIG, parse_env_config=True)
    assert "default_true" in tap.config
    assert "default_false" in tap.config
    assert tap.config["default_true"] is True
    assert tap.config["default_false"] is False
