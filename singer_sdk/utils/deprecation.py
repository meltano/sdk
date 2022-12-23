"""Utilities for deprecating features."""

__all__ = ["SingerSDKDeprecationWarning"]


class SingerSDKDeprecationWarning(DeprecationWarning):
    """Warning to raise when a deprecated feature is used."""
