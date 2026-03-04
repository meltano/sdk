"""Compatibility helpers."""

from __future__ import annotations

import datetime
import sys
import typing as t
import warnings
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


class SingerSDKPendingDeprecationWarning(PendingDeprecationWarning):
    """Warning for deprecated features without a committed removal timeline."""


def singer_sdk_deprecated(
    msg: str,
    *,
    removal_version: str,
    stacklevel: int = 1,
) -> t.Any:  # noqa: ANN401
    """Deprecation decorator that requires an explicit removal version.

    Args:
        msg: Human-readable message. Should describe the replacement but need
             not mention the version — it is appended automatically.
        removal_version: SDK version when this will be removed (e.g. ``"v0.58"``
            or ``"1.0"``).
        stacklevel: Passed through to :func:`deprecated`.

    Returns:
        A decorator that marks the target as deprecated.
    """
    return deprecated(
        f"{msg} Will be removed in {removal_version}.",  # ty:ignore[invalid-argument-type]
        category=SingerSDKDeprecationWarning,
        stacklevel=stacklevel,
    )


class SingerSDKPythonEOLWarning(FutureWarning):
    """Warning issued when the running Python is near or past its end of life."""


# EOL dates from https://devguide.python.org/versions/
_PYTHON_EOL_DATES: dict[tuple[int, int], datetime.date] = {
    (3, 10): datetime.date(2026, 10, 4),
    (3, 11): datetime.date(2027, 10, 24),
    (3, 12): datetime.date(2028, 10, 2),
    (3, 13): datetime.date(2029, 10, 7),
    (3, 14): datetime.date(2030, 10, 7),
}
_PYTHON_EOL_WARNING_PERIOD = datetime.timedelta(days=365)  # warn 1 year before EOL


def warn_python_eol(
    _today: datetime.date | None = None,
    _version: tuple[int, int] | None = None,
) -> None:
    """Issue a SingerSDKPythonEOLWarning if the running Python is near/past EOL.

    Args:
        _today: Override today's date (testing only).
        _version: Override the Python version tuple (testing only).
    """
    version = _version or sys.version_info[:2]
    eol = _PYTHON_EOL_DATES.get(version)
    if eol is None:
        return

    today = _today or datetime.datetime.now(datetime.timezone.utc).date()
    py_str = f"Python {version[0]}.{version[1]}"

    if today >= eol:
        warnings.warn(
            f"{py_str} reached its end of life on {eol}. "
            "The Singer SDK may drop support for it in an upcoming release. "
            "Please upgrade to a supported Python version. "
            "See https://devguide.python.org/versions/ for details.",
            SingerSDKPythonEOLWarning,
            stacklevel=2,
        )
    elif today >= eol - _PYTHON_EOL_WARNING_PERIOD:
        warnings.warn(
            f"{py_str} will reach its end of life on {eol}. "
            "The Singer SDK will drop support for it in an upcoming release. "
            "Please plan to upgrade to a supported Python version. "
            "See https://devguide.python.org/versions/ for details.",
            SingerSDKPythonEOLWarning,
            stacklevel=2,
        )


__all__ = [
    "SingerSDKDeprecationWarning",
    "SingerSDKPendingDeprecationWarning",
    "SingerSDKPythonEOLWarning",
    "Traversable",
    "date_fromisoformat",
    "datetime_fromisoformat",
    "deprecated",
    "entry_points",
    "importlib_resources",
    "singer_sdk_deprecated",
    "time_fromisoformat",
    "warn_python_eol",
]
