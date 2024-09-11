from __future__ import annotations

import warnings
from inspect import currentframe, getframeinfo

import pytest

from singer_sdk.helpers.capabilities import CapabilitiesEnum


class DummyCapabilitiesEnum(CapabilitiesEnum):
    """Simple capabilities enumeration."""

    MY_SUPPORTED_FEATURE = "supported"
    MY_DEPRECATED_FEATURE = "deprecated", "No longer supported."


def test_deprecated_capabilities():
    # Dictionary access
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        _ = DummyCapabilitiesEnum["MY_SUPPORTED_FEATURE"]

    # Call
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        _ = DummyCapabilitiesEnum("supported")

    # Attribute access
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        _ = DummyCapabilitiesEnum.MY_SUPPORTED_FEATURE

    # Dictionary access
    with pytest.warns(
        DeprecationWarning,
        match="is deprecated. No longer supported",
    ) as record:
        _ = DummyCapabilitiesEnum["MY_DEPRECATED_FEATURE"]

    # Call
    with pytest.warns(
        DeprecationWarning,
        match="is deprecated. No longer supported",
    ) as record:
        DummyCapabilitiesEnum("deprecated")

    # Attribute access
    with pytest.warns(
        DeprecationWarning,
        match="is deprecated. No longer supported",
    ) as record:
        _ = DummyCapabilitiesEnum.MY_DEPRECATED_FEATURE

    warning = record.list[0]
    frameinfo = getframeinfo(currentframe())
    assert warning.lineno == frameinfo.lineno - 3
    assert warning.filename.endswith("test_capabilities.py")

    with pytest.warns(
        DeprecationWarning,
        match="is deprecated. No longer supported",
    ) as record:
        DummyCapabilitiesEnum("deprecated")

    warning = record.list[0]
    frameinfo = getframeinfo(currentframe())
    assert warning.lineno == frameinfo.lineno - 3
    assert warning.filename.endswith("test_capabilities.py")
