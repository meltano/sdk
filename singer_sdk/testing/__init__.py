"""Tools and standard tests for Tap/Target implementations."""

from __future__ import annotations

__lazy_modules__ = {
    "singer_sdk.testing.config",
    "singer_sdk.testing.factory",
    "singer_sdk.testing.legacy",
    "singer_sdk.testing.runners",
    "typing",
    "warnings",
}

import typing as t
import warnings

from singer_sdk.testing.config import SuiteConfig
from singer_sdk.testing.factory import get_tap_test_class, get_target_test_class
from singer_sdk.testing.legacy import (
    _get_tap_catalog,
    _select_all,
    sync_end_to_end,
    tap_sync_test,
    tap_to_target_sync_test,
    target_sync_test,
)
from singer_sdk.testing.runners import SingerTestRunner, TapTestRunner, TargetTestRunner


def __getattr__(name: str) -> t.Any:  # noqa: ANN401
    if name == "get_standard_tap_tests":
        warnings.warn(
            "The function singer_sdk.testing.get_standard_tap_tests is deprecated "
            "and will be removed in a future release. Use get_tap_test_class instead.",
            DeprecationWarning,
            stacklevel=2,
        )

        from .legacy import get_standard_tap_tests  # noqa: PLC0415

        return get_standard_tap_tests

    if name == "get_standard_target_tests":
        warnings.warn(
            "The function singer_sdk.testing.get_standard_target_tests is deprecated "
            "and will be removed in a future release. Use get_target_test_class "
            "instead.",
            DeprecationWarning,
            stacklevel=2,
        )

        from .legacy import get_standard_target_tests  # noqa: PLC0415

        return get_standard_target_tests

    msg = f"module {__name__} has no attribute {name}"
    raise AttributeError(msg)


__all__ = [
    "SingerTestRunner",
    "SuiteConfig",
    "TapTestRunner",
    "TargetTestRunner",
    "_get_tap_catalog",
    "_select_all",
    "get_tap_test_class",
    "get_target_test_class",
    "sync_end_to_end",
    "tap_sync_test",
    "tap_to_target_sync_test",
    "target_sync_test",
]
