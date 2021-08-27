"""Pre-built test functions which can be applied to multiple taps."""

from typing import Type, Callable, List

from singer_sdk.tap_base import Tap


def get_standard_tap_tests(tap_class: Type[Tap], config=None) -> List[Callable]:
    """Return callable pytest which executes simple discovery and connection tests."""

    def _test_cli_prints():
        # Initialize with basic config
        tap1: Tap = tap_class(config=config, parse_env_config=True)
        # Test CLI prints
        tap1.print_version()
        tap1.print_about()
        tap1.print_about(format="json")

    def _test_discovery():
        # Initialize with basic config
        tap1: Tap = tap_class(config=config, parse_env_config=True)
        # Test discovery
        tap1.run_discovery()
        catalog1 = tap1.catalog_dict
        # Reset and re-initialize with an input catalog
        tap1 = None
        tap2: Tap = tap_class(config=config, parse_env_config=True, catalog=catalog1)
        assert tap2

    def _test_stream_connections():
        # Initialize with basic config
        tap1: Tap = tap_class(config=config, parse_env_config=True)
        tap1.run_connection_test()

    return [_test_cli_prints, _test_discovery, _test_stream_connections]


def get_standard_target_tests(target_class: Type[Tap], config=None) -> List[Callable]:
    """Return callable pytest which executes simple discovery and connection tests."""
    return []
