"""Type aliases for use in the SDK."""

from __future__ import annotations

import os
import typing as t
from collections.abc import Mapping

import requests

if t.TYPE_CHECKING:
    import sys

    if sys.version_info >= (3, 10):
        from typing import TypeAlias  # noqa: ICN003
    else:
        from typing_extensions import TypeAlias


__all__ = [
    "Context",
    "Record",
]

Context: TypeAlias = Mapping[str, t.Any]
Record: TypeAlias = dict[str, t.Any]
Auth: TypeAlias = t.Callable[[requests.PreparedRequest], requests.PreparedRequest]
StrPath: TypeAlias = t.Union[str, os.PathLike[str]]


class TapState(t.TypedDict, total=False):
    """Tap state."""

    bookmarks: dict[str, dict[str, t.Any]]
