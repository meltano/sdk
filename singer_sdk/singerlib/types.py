"""Reusable type annotations for Singer objects."""

from __future__ import annotations

__lazy_modules__ = {"typing"}

import typing as t

KeyProperties: t.TypeAlias = list[str] | tuple[str, ...]
