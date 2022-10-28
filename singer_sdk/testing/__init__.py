"""SDK Standard Tap and Target Tests."""


from .target import StandardSqlTargetTests

# compatibility test imports
from .testing import (
    _get_tap_catalog,
    _select_all,
    get_standard_tap_tests,
    get_standard_target_tests,
    sync_end_to_end,
    tap_sync_test,
    tap_to_target_sync_test,
    target_sync_test,
)

__all__ = [
    "StandardSqlTargetTests",
    "_get_tap_catalog",
    "_select_all",
    "get_standard_tap_tests",
    "get_standard_target_tests",
    "sync_end_to_end",
    "tap_sync_test",
    "tap_to_target_sync_test",
    "target_sync_test",
]
