"""Type aliases for use in the SDK."""

from __future__ import annotations

import sys
import typing as t
from collections.abc import Mapping

import requests

if sys.version_info < (3, 10):
    from typing_extensions import TypeAlias
else:
    from typing import TypeAlias  # noqa: ICN003


__all__ = [
    "Context",
    "Record",
]

Context: TypeAlias = Mapping[str, t.Any]
Record: TypeAlias = dict[str, t.Any]
Auth: TypeAlias = t.Callable[[requests.PreparedRequest], requests.PreparedRequest]
