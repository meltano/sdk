# flake8: noqa

"""Defines the `classproperty` decorator."""

from __future__ import annotations


class classproperty(property):
    """Class property decorator."""

    def __get__(self, obj, objtype=None):
        return super().__get__(objtype)

    def __set__(self, obj, value):
        super().__set__(type(obj), value)

    def __delete__(self, obj):
        super().__delete__(type(obj))
