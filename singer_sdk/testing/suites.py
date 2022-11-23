"""Standard Tap and Target test suites."""

from __future__ import annotations

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
    StreamRecordSchemaMatchesCatalogTest,
    StreamReturnsRecordTest,
    TapCLIPrintsTest,
    TapDiscoveryTest,
    TapStreamConnectionTest,
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
    TargetSchemaNoProperties,
    TargetSchemaUpdates,
    TargetSpecialCharsInAttributes,
)
from .templates import TapTestTemplate, TargetTestTemplate, TestTemplate


@dataclass
class TestSuite:
    """Test Suite container class."""

    type: str
    tests: list[type[TestTemplate] | type[TapTestTemplate] | type[TargetTestTemplate]]


# Tap Test Suites
tap_tests = TestSuite(
    type="tap", tests=[TapCLIPrintsTest, TapDiscoveryTest, TapStreamConnectionTest]
)
tap_stream_tests = TestSuite(
    type="tap_stream",
    tests=[
        StreamCatalogSchemaMatchesRecordTest,
        StreamRecordSchemaMatchesCatalogTest,
        StreamReturnsRecordTest,
        StreamPrimaryKeysTest,
    ],
)
tap_stream_attribute_tests = TestSuite(
    type="tap_stream_attribute",
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
    type="target",
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
        TargetSchemaNoProperties,
        TargetSchemaUpdates,
        TargetSpecialCharsInAttributes,
    ],
)
