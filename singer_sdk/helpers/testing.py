"""Pre-built test functions which can be applied to multiple taps."""

from singer_sdk.tap_base import Tap
from typing import Type, Callable


def get_basic_tap_test(tap_class: Type[Tap], tap_config=None) -> Callable:
    """Return callable pytest which executes simple discovery and connection tests."""

    def _test_fn(config=None):
        # Initialize with basic config
        tap1: Tap = tap_class(config=config or tap_config, parse_env_config=True)
        # Test CLI prints
        tap1.print_version()
        tap1.print_about()
        tap1.print_about(format="json")
        # Test discovery
        tap1.run_discovery()
        catalog1 = tap1.catalog_dict
        # Reset and re-initialize with an input catalog
        tap1 = None
        tap2: Tap = tap_class(
            config=config or tap_config, parse_env_config=True, catalog=catalog1
        )
        # Run data sync test on all streams
        tap2.run_connection_test()

    return _test_fn
