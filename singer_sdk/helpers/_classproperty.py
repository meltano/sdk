"""Defines the `classproperty` decorator."""

from __future__ import annotations


class classproperty(property):  # noqa: N801
    """Class property decorator."""

    def __get__(self, obj, objtype=None):  # noqa: ANN001, ANN204
        return super().__get__(objtype)

    def __set__(self, obj, value):  # noqa: ANN001, ANN204
        super().__set__(type(obj), value)

    def __delete__(self, obj):  # noqa: ANN001, ANN204
        super().__delete__(type(obj))
