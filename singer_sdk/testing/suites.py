"""Standard Tap and Target test suites."""

from __future__ import annotations

import typing as t
from dataclasses import dataclass

from .tap_tests import (
    AttributeIsBooleanTest,
    AttributeIsDateTimeTest,
    AttributeIsIntegerTest,
    AttributeIsNumberTest,
    AttributeIsObjectTest,
    AttributeNotNullTest,
    StreamCatalogSchemaMatchesRecordTest,
    StreamPrimaryKeysTest,
    StreamRecordMatchesStreamSchema,
    StreamRecordSchemaMatchesCatalogTest,
    StreamReturnsRecordTest,
    StreamSchemaIsValidTest,
    TapCLIPrintsTest,
    TapDiscoveryTest,
    TapStreamConnectionTest,
    TapValidFinalStateTest,
)

# TODO: add TargetMultipleStateMessages
# TODO: fix behavior in SDK to make this pass
from .target_tests import (
    TargetArrayData,
    TargetCamelcaseComplexSchema,
    TargetCamelcaseTest,
    TargetCliPrintsTest,
    TargetDuplicateRecords,
    TargetEncodedStringData,
    TargetInvalidSchemaTest,
    TargetNoPrimaryKeys,
    TargetOptionalAttributes,
    TargetPrimaryKeyUpdates,
    TargetRecordBeforeSchemaTest,
    TargetRecordMissingKeyProperty,
    TargetRecordMissingOptionalFields,
    TargetSchemaNoProperties,
    TargetSchemaUpdates,
    TargetSpecialCharsInAttributes,
)
from .templates import TestTemplate

T = t.TypeVar("T", bound=TestTemplate)


def __getattr__(name: str) -> t.Any:  # noqa: ANN401
    """Provide backwards compatibility for TestSuite."""  # noqa: DOC201, DOC501
    if name == "TestSuite":
        import warnings  # noqa: PLC0415

        warnings.warn(
            "TestSuite is deprecated, use SingerTestSuite instead",
            DeprecationWarning,
            stacklevel=2,
        )
        return SingerTestSuite
    msg = f"module '{__name__}' has no attribute '{name}'"
    raise AttributeError(msg)


@dataclass(slots=True)
class SingerTestSuite(t.Generic[T]):
    """Test Suite container class."""

    kind: t.Literal[
        "tap",
        "tap_stream",
        "tap_stream_attribute",
        "target",
    ]
    tests: list[type[T]]


# Tap Test Suites
tap_tests = SingerTestSuite(
    kind="tap",
    tests=[
        TapCLIPrintsTest,
        TapDiscoveryTest,
        TapStreamConnectionTest,
        TapValidFinalStateTest,
    ],
)
tap_stream_tests = SingerTestSuite(
    kind="tap_stream",
    tests=[
        StreamCatalogSchemaMatchesRecordTest,
        StreamRecordMatchesStreamSchema,
        StreamRecordSchemaMatchesCatalogTest,
        StreamReturnsRecordTest,
        StreamSchemaIsValidTest,
        StreamPrimaryKeysTest,
    ],
)
tap_stream_attribute_tests = SingerTestSuite(
    kind="tap_stream_attribute",
    tests=[
        AttributeIsBooleanTest,
        AttributeIsDateTimeTest,
        AttributeIsIntegerTest,
        AttributeIsNumberTest,
        AttributeIsObjectTest,
        AttributeNotNullTest,
    ],
)


# Target Test Suites
target_tests = SingerTestSuite(
    kind="target",
    tests=[
        TargetArrayData,
        TargetCamelcaseComplexSchema,
        TargetCamelcaseTest,
        TargetCliPrintsTest,
        TargetDuplicateRecords,
        TargetEncodedStringData,
        TargetInvalidSchemaTest,
        # TargetMultipleStateMessages,
        TargetNoPrimaryKeys,
        TargetOptionalAttributes,
        TargetPrimaryKeyUpdates,
        TargetRecordBeforeSchemaTest,
        TargetRecordMissingKeyProperty,
        TargetRecordMissingOptionalFields,
        TargetSchemaNoProperties,
        TargetSchemaUpdates,
        TargetSpecialCharsInAttributes,
    ],
)
