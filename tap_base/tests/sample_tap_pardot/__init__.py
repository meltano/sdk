"""Module test for tap-pardot functionality."""

from tap_base.tests.sample_tap_pardot.tap import SampleTapPardot
from tap_base.tests.sample_tap_pardot.stream import SampleTapPardotStream
from tap_base.tests.sample_tap_pardot.connection import SampleTapPardotConnection

__all__ = [
    "SampleTapPardot",
    "SampleTapPardotStream",
    "SampleTapPardotConnection",
]
