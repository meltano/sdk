"""Useful wrappers for common Python types."""

from __future__ import annotations

import sys
from os import PathLike
from typing import Any, Callable, TypeVar, Union

from singer.schema import Schema

if sys.version_info >= (3, 9):
    from importlib.abc import Traversable
else:
    from importlib_resources.abc import Traversable


_T = TypeVar("_T")
_MaybeCallable = Union[_T, Callable[[], _T]]
_StreamSchemaInput = Union[str, PathLike, dict[str, Any], Schema, Traversable]
