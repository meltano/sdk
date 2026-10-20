"""Helpers for batch tests."""

from __future__ import annotations

import importlib
import typing as t

if t.TYPE_CHECKING:
    import pytest


def block_import(monkeypatch: pytest.MonkeyPatch, package: str) -> None:
    """Make importing ``package`` raise ``ModuleNotFoundError``."""
    real_import_module = importlib.import_module

    def import_module(name: str, *args: t.Any, **kwargs: t.Any) -> t.Any:
        if name.partition(".")[0] == package:
            msg = f"No module named {package!r}"
            raise ModuleNotFoundError(msg, name=package)
        return real_import_module(name, *args, **kwargs)

    monkeypatch.setattr("singer_sdk.batch.importlib.import_module", import_module)
