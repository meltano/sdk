from __future__ import annotations

import logging
import typing as t
import warnings

import pytest

from singer_sdk._logging import _setup_warning_logging

if t.TYPE_CHECKING:
    from collections.abc import Iterator


@pytest.fixture
def restore_showwarning() -> Iterator[None]:
    original = warnings.showwarning
    yield
    warnings.showwarning = original


@pytest.mark.usefixtures("restore_showwarning")
def test_warnings_are_logged_with_structured_extras(
    caplog: pytest.LogCaptureFixture,
):
    with (
        warnings.catch_warnings(),
        caplog.at_level(
            logging.WARNING,
            logger="singer_sdk.warnings",
        ),
    ):
        warnings.simplefilter("always")
        _setup_warning_logging()
        warnings.warn("old thing", DeprecationWarning, stacklevel=1)

    (record,) = [r for r in caplog.records if r.name == "singer_sdk.warnings"]
    assert record.getMessage() == "DeprecationWarning: old thing"
    assert record.warning_category == "DeprecationWarning"  # type: ignore[attr-defined]
    assert record.warning_source.startswith(__file__)  # type: ignore[attr-defined]


@pytest.mark.usefixtures("restore_showwarning")
def test_setup_is_reversible():
    original = warnings.showwarning

    restore = _setup_warning_logging()
    assert warnings.showwarning is not original

    # Repeated setup must not clobber the saved hook
    _setup_warning_logging()()
    assert warnings.showwarning is not original

    restore()
    assert warnings.showwarning is original
