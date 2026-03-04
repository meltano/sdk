"""Tests for singer_sdk/helpers/_compat.py."""

from __future__ import annotations

import datetime
import warnings

from singer_sdk.helpers._compat import (
    SingerSDKPythonEOLWarning,
    warn_python_eol,
)

# ---------------------------------------------------------------------------
# warn_python_eol
# ---------------------------------------------------------------------------


def test_warn_python_eol_no_warning_far_from_eol() -> None:
    """No warning when today is well before the 1-year EOL window."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        warn_python_eol(
            _today=datetime.date(2024, 1, 1),
            _version=(3, 10),
        )

    assert not caught


def test_warn_python_eol_approaching() -> None:
    """A FutureWarning is issued when within the 1-year window before EOL."""
    # 3.10 EOL is 2026-10-04; pick a date 6 months before
    approaching = datetime.date(2026, 4, 4)

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        warn_python_eol(_today=approaching, _version=(3, 10))

    assert len(caught) == 1
    assert issubclass(caught[0].category, SingerSDKPythonEOLWarning)
    assert "will reach its end of life" in str(caught[0].message)


def test_warn_python_eol_past_eol() -> None:
    """A FutureWarning is issued when today is past the EOL date."""
    past_eol = datetime.date(2027, 1, 1)

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        warn_python_eol(_today=past_eol, _version=(3, 10))

    assert len(caught) == 1
    assert issubclass(caught[0].category, SingerSDKPythonEOLWarning)
    assert "reached its end of life" in str(caught[0].message)


def test_warn_python_eol_unknown_version() -> None:
    """No warning is issued for a Python version not in the EOL table."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        warn_python_eol(_today=datetime.date(2030, 1, 1), _version=(3, 99))

    assert not caught
