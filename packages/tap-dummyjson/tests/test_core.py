from __future__ import annotations

from singer_sdk.testing import get_tap_test_class

from tap_dummyjson.tap import TapDummyJSON

CONFIG = {
    "username": "emilys",
    "password": "emilyspass",
}

TestTapDummyJSON = get_tap_test_class(tap_class=TapDummyJSON, config=CONFIG)
