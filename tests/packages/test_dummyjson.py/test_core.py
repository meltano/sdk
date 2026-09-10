from __future__ import annotations

from tap_dummyjson.tap import TapDummyJSON

from singer_sdk.testing import get_tap_test_class

CONFIG = {
    "username": "emilys",
    "password": "emilyspass",
    "start_date": "2026-08-01",
}

TestTapDummyJSON = get_tap_test_class(tap_class=TapDummyJSON, config=CONFIG)
