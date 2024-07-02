"""Test the BuiltinSetting descriptor."""

from __future__ import annotations

from singer_sdk.helpers._config_property import ConfigProperty


def test_builtin_setting_descriptor():
    class ObjWithConfig:
        example = ConfigProperty(default=1)

        def __init__(self):
            self.config = {"example": 1}

    obj = ObjWithConfig()
    assert obj.example == 1

    obj.config["example"] = 2
    assert obj.example == 2


def test_builtin_setting_descriptor_custom_key():
    class ObjWithConfig:
        my_attr = ConfigProperty("example", default=1)

        def __init__(self):
            self.config = {"example": 1}

    obj = ObjWithConfig()
    assert obj.my_attr == 1

    obj.config["example"] = 2
    assert obj.my_attr == 2
