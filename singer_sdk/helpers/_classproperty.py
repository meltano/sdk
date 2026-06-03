"""Defines the `classproperty` decorator."""

from __future__ import annotations

import sys

if sys.version_info >= (3, 12):
    from typing import override  # noqa: ICN003
else:
    from typing_extensions import override


class classproperty(property):  # noqa: N801
    """Class property decorator."""

    @override
    def __get__(self, obj, objtype=None):  # noqa: ANN001, ANN204
        return super().__get__(objtype)

    @override
    def __set__(self, obj, value):  # noqa: ANN001, ANN204
        super().__set__(type(obj), value)

    @override
    def __delete__(self, obj):  # noqa: ANN001, ANN204
        super().__delete__(type(obj))
