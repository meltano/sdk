"""Pre-built test functions which can be applied to multiple taps."""

from singer_sdk.tap_base import Tap
from typing import Type, Callable


def get_basic_tap_test(tap_class: Type[Tap], tap_config=None) -> Callable:
    """Return callable pytest which executes simple discovery and connection tests."""

    def _test_fn(config=None):
        tap = tap_class(config=config or tap_config, parse_env_config=True)
        tap.run_discovery()
        tap.run_connection_test()

    return _test_fn
