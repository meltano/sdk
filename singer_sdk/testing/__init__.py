"""Tools and standard tests for Tap/Target implementations."""

from .config import SuiteConfig
from .factory import get_tap_test_class, get_target_test_class, get_test_class
from .legacy import (
    _get_tap_catalog,
    _select_all,
    get_standard_tap_tests,
    get_standard_target_tests,
    sync_end_to_end,
    tap_sync_test,
    tap_to_target_sync_test,
    target_sync_test,
)
from .runners import SingerTestRunner, TapTestRunner, TargetTestRunner

__all__ = [
    "get_tap_test_class",
    "get_target_test_class",
    "get_test_class",
    "_get_tap_catalog",
    "_select_all",
    "get_standard_tap_tests",
    "get_standard_target_tests",
    "sync_end_to_end",
    "tap_sync_test",
    "tap_to_target_sync_test",
    "target_sync_test",
    "SingerTestRunner",
    "TapTestRunner",
    "TargetTestRunner",
    "SuiteConfig",
]
