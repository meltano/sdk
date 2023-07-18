"""Tests standard tap features using the built-in SDK tests library."""

from __future__ import annotations

import warnings

from samples.sample_tap_google_analytics.ga_tap import SampleTapGoogleAnalytics
from singer_sdk.exceptions import ConfigValidationError
from singer_sdk.testing import get_tap_test_class

from .conftest import ga_config

try:
    TestSampleTapGoogleAnalytics = get_tap_test_class(
        tap_class=SampleTapGoogleAnalytics,
        config=ga_config(),
        parse_env_config=True,
    )
except ConfigValidationError as e:
    warnings.warn(
        UserWarning(
            "Could not configure external gitlab tests. "
            f"Config in CI is expected via env vars.\n{e}",
        ),
        stacklevel=2,
    )
