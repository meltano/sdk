from __future__ import annotations

from samples.sample_tap_dummy_json.tap_dummyjson.tap import TapDummyJSON
from singer_sdk.testing import get_tap_test_class

CONFIG = {
    "username": "emilys",
    "password": "emilyspass",
}

TestTapDummyJSON = get_tap_test_class(tap_class=TapDummyJSON, config=CONFIG)
