"""Test the plugin testing helpers."""

from __future__ import annotations

import pytest


def test_module_deprecations():
    with pytest.deprecated_call():
        from singer_sdk.testing import get_standard_tap_tests  # noqa: F401

    with pytest.deprecated_call():
        from singer_sdk.testing import get_standard_target_tests  # noqa: F401

    from singer_sdk import testing

    with pytest.raises(
        AttributeError,
        match="module singer_sdk.testing has no attribute",
    ):
        testing.foo  # noqa: B018
