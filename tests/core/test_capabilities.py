from inspect import currentframe, getframeinfo

import pytest

from singer_sdk.helpers.capabilities import CapabilitiesEnum


class DummyCapabilitiesEnum(CapabilitiesEnum):
    """Simple capabilities enumeration."""

    MY_SUPPORTED_FEATURE = "supported"
    MY_DEPRECATED_FEATURE = "deprecated", "No longer supported."


def test_deprecated_capabilities():
    with pytest.warns(None) as record:
        DummyCapabilitiesEnum.MY_SUPPORTED_FEATURE

    assert len(record.list) == 0

    with pytest.warns(
        DeprecationWarning,
        match="is deprecated. No longer supported",
    ) as record:
        DummyCapabilitiesEnum.MY_DEPRECATED_FEATURE

    warning = record.list[0]
    frameinfo = getframeinfo(currentframe())
    assert warning.lineno == frameinfo.lineno - 3
    assert warning.filename.endswith("test_capabilities.py")
