"""Test tap-to-target sync."""

from tap_base.tests.sample_tap_parquet.tap import SampleTapParquet
from tap_base.tests.sample_target_parquet.target import SampleTargetParquet

TAP_CONFIG = {}


def test_tap_to_target():
    tap = SampleTapParquet(config=TAP_CONFIG, state=None)
    target = SampleTargetParquet(config=TAP_CONFIG, state=None)
    tap_output: str = tap.sync_all(allow_load=False, allow_discover=True)
    target.process_lines(tap_output.splitlines())
