"""Test the plugin testing helpers."""

from __future__ import annotations

from singer_sdk.testing.factory import BaseTestClass


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
