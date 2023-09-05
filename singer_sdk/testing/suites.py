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
    TargetRecordBeforeSchemaTest,
    TargetRecordMissingKeyProperty,
    TargetRecordMissingOptionalFields,
    TargetSchemaNoProperties,
    TargetSchemaUpdates,
    TargetSpecialCharsInAttributes,
)

if t.TYPE_CHECKING:
    from .templates import TapTestTemplate, TargetTestTemplate, TestTemplate


@dataclass
class TestSuite:
    """Test Suite container class."""

    kind: str
    tests: list[type[TestTemplate] | type[TapTestTemplate] | type[TargetTestTemplate]]


# Tap Test Suites
tap_tests = TestSuite(
    kind="tap",
    tests=[
        TapCLIPrintsTest,
        TapDiscoveryTest,
        TapStreamConnectionTest,
        TapValidFinalStateTest,
    ],
)
tap_stream_tests = TestSuite(
    kind="tap_stream",
    tests=[
        StreamCatalogSchemaMatchesRecordTest,
        StreamRecordMatchesStreamSchema,
        StreamRecordSchemaMatchesCatalogTest,
        StreamReturnsRecordTest,
        StreamPrimaryKeysTest,
    ],
)
tap_stream_attribute_tests = TestSuite(
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
target_tests = TestSuite(
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
        TargetRecordBeforeSchemaTest,
        TargetRecordMissingKeyProperty,
        TargetRecordMissingOptionalFields,
        TargetSchemaNoProperties,
        TargetSchemaUpdates,
        TargetSpecialCharsInAttributes,
    ],
)
