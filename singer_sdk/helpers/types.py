"""Type aliases for use in the SDK."""

from __future__ import annotations

import os
import typing as t
from collections.abc import Mapping

import requests

__all__ = [
    "Context",
    "Record",
]

Context: t.TypeAlias = Mapping[str, t.Any]
Record: t.TypeAlias = dict[str, t.Any]
Auth: t.TypeAlias = t.Callable[[requests.PreparedRequest], requests.PreparedRequest]
RequestFunc: t.TypeAlias = t.Callable[
    [requests.PreparedRequest, Context | None],
    requests.Response,
]
StrPath: t.TypeAlias = str | os.PathLike[str]


class TapState(t.TypedDict, total=False):
    """Tap state."""

    bookmarks: dict[str, dict[str, t.Any]]
