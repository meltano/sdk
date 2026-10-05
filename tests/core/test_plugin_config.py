"""Test plugin config functions."""

from __future__ import annotations

import sys
import typing as t

from singer_sdk.tap_base import Tap
from singer_sdk.typing import BooleanType, PropertiesList, Property

if sys.version_info >= (3, 12):
    from typing import override  # noqa: ICN003
else:
    from typing_extensions import override

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

    @override
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
