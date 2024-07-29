"""Type aliases for use in the SDK."""

from __future__ import annotations

import sys
import typing as t

if sys.version_info < (3, 9):
    from typing import Mapping  # noqa: ICN003
else:
    from collections.abc import Mapping

if sys.version_info < (3, 10):
    from typing_extensions import TypeAlias
else:
    from typing import TypeAlias  # noqa: ICN003

__all__ = [
    "Context",
    "Record",
]

Context: TypeAlias = Mapping[str, t.Any]
Record: TypeAlias = t.Dict[str, t.Any]
