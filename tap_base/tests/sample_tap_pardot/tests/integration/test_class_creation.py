"""Test class creation."""

from tap_base.tests.sample_tap_pardot.tap import SampleTapPardot


def test_tap_class():
    """Test class creation."""
    _ = SampleTapPardot(config=None, state=None)
