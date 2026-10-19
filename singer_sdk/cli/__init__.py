"""Helpers for the tap, target and mapper CLIs."""

from __future__ import annotations

__lazy_modules__ = {"singer_sdk.cli._decorator"}

from singer_sdk.cli._decorator import plugin_cli

__all__ = ["plugin_cli"]
