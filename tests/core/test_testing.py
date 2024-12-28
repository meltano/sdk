"""Test the plugin testing helpers."""

from __future__ import annotations

import pytest

from singer_sdk.testing.factory import BaseTestClass


def test_module_deprecations():
    with pytest.deprecated_call():
        from singer_sdk.testing import get_standard_tap_tests  # noqa: F401, PLC0415

    with pytest.deprecated_call():
        from singer_sdk.testing import get_standard_target_tests  # noqa: F401, PLC0415

    from singer_sdk import testing  # noqa: PLC0415

    with pytest.raises(
        AttributeError,
        match="module singer_sdk\\.testing has no attribute",
    ):
        testing.foo  # noqa: B018


def test_test_class_mro():
    class PluginTestClass(BaseTestClass):
        pass

    PluginTestClass.params["x"] = 1

    class AnotherPluginTestClass(BaseTestClass):
        pass

    AnotherPluginTestClass.params["x"] = 2
    AnotherPluginTestClass.params["y"] = 3

    class SubPluginTestClass(PluginTestClass):
        pass

    assert PluginTestClass.params == {"x": 1}
    assert AnotherPluginTestClass.params == {"x": 2, "y": 3}
    assert SubPluginTestClass.params == {"x": 1}
