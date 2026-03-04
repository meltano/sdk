"""Tests for singer_sdk/helpers/_compat.py."""

from __future__ import annotations

import datetime
import warnings

import pytest

from singer_sdk.helpers._compat import (
    SingerSDKDeprecationWarning,
    SingerSDKPythonEOLWarning,
    singer_sdk_deprecated,
    warn_python_eol,
)

# ---------------------------------------------------------------------------
# singer_sdk_deprecated
# ---------------------------------------------------------------------------


def test_singer_sdk_deprecated_requires_removal_version() -> None:
    """singer_sdk_deprecated must be called with removal_version keyword."""
    with pytest.raises(TypeError):
        singer_sdk_deprecated("Some message")  # type: ignore[call-arg]


def test_singer_sdk_deprecated_message_contains_version() -> None:
    """The emitted warning message must include the removal_version."""

    @singer_sdk_deprecated("Old thing is gone. Use new thing.", removal_version="v0.99")
    def _old_func() -> None:
        pass

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        _old_func()

    assert len(caught) == 1
    assert "v0.99" in str(caught[0].message)


def test_singer_sdk_deprecated_message_contains_guidance() -> None:
    """The emitted warning message must include the original guidance text."""

    @singer_sdk_deprecated("Use NewClass instead.", removal_version="v1.0")
    class _OldClass:
        pass

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        _OldClass()

    assert len(caught) == 1
    assert "Use NewClass instead." in str(caught[0].message)
    assert issubclass(caught[0].category, SingerSDKDeprecationWarning)


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
