"""Compatibility helpers."""

from __future__ import annotations

import datetime
import sys
from importlib import resources as importlib_resources

if sys.version_info >= (3, 13):
    from warnings import deprecated
else:
    from typing_extensions import deprecated

if sys.version_info >= (3, 12):
    from importlib.resources.abc import Traversable
else:
    from importlib.abc import Traversable

if sys.version_info >= (3, 12):
    from importlib.metadata import entry_points
else:
    from importlib_metadata import entry_points

if sys.version_info < (3, 11):
    from backports.datetime_fromisoformat import MonkeyPatch

    MonkeyPatch.patch_fromisoformat()

datetime_fromisoformat = datetime.datetime.fromisoformat
date_fromisoformat = datetime.date.fromisoformat
time_fromisoformat = datetime.time.fromisoformat


class SingerSDKDeprecationWarning(DeprecationWarning):
    """Custom deprecation warning for the Singer SDK."""


__all__ = [
    "SingerSDKDeprecationWarning",
    "Traversable",
    "date_fromisoformat",
    "datetime_fromisoformat",
    "deprecated",
    "entry_points",
    "importlib_resources",
    "time_fromisoformat",
]
