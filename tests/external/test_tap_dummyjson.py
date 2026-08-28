from __future__ import annotations

import pytest
from tap_dummyjson.tap import TapDummyJSON

from singer_sdk.testing import SuiteConfig, get_tap_test_class

CONFIG = {
    "username": "emilys",
    "password": "emilyspass",
}

# See `test_tap_gitlab.py` for why `vcr_cassette` (not plain `@pytest.mark.vcr`) is
# required here.
TestTapDummyJSON = pytest.mark.vcr_cassette("dummyjson.yaml")(
    get_tap_test_class(
        tap_class=TapDummyJSON,
        config=CONFIG,
        suite_config=SuiteConfig(max_records_limit=60),
    ),
)
