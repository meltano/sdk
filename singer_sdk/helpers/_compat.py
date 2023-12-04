"""Compatibility helpers."""

from __future__ import annotations

import datetime
import sys

if sys.version_info < (3, 8):
    import importlib_metadata as metadata
    from typing_extensions import final
else:
    from importlib import metadata
    from typing import final  # noqa: ICN003

if sys.version_info < (3, 12):
    from importlib_metadata import entry_points
else:
    from importlib.metadata import entry_points

if sys.version_info < (3, 9):
    import importlib_resources as resources
else:
    from importlib import resources

if sys.version_info < (3, 11):
    from backports.datetime_fromisoformat import MonkeyPatch

    MonkeyPatch.patch_fromisoformat()

datetime_fromisoformat = datetime.datetime.fromisoformat
date_fromisoformat = datetime.date.fromisoformat
time_fromisoformat = datetime.time.fromisoformat

__all__ = [
    "metadata",
    "final",
    "resources",
    "entry_points",
    "datetime_fromisoformat",
    "date_fromisoformat",
    "time_fromisoformat",
]
