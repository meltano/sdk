from dataclasses import dataclass
from typing import List

from .tap_tests import (
    AttributeIsBooleanTest,
    AttributeIsDateTimeTest,
    AttributeIsIntegerTest,
    AttributeIsNumberTest,
    AttributeIsObjectTest,
    AttributeNotNullTest,
    AttributeUniquenessTest,
    StreamCatalogSchemaMatchesRecordTest,
    StreamPrimaryKeysTest,
    StreamRecordSchemaMatchesCatalogTest,
    StreamReturnsRecordTest,
    TapCLIPrintsTest,
    TapDiscoveryTest,
    TapStreamConnectionTest,
)
from .target_tests import (
    TargetArrayData,
    TargetCamelcaseTest,
    TargetCliPrintsTest,
    TargetDuplicateRecords,
    TargetEncodedStringData,
    TargetInvalidSchemaTest,
    TargetMultipleStateMessages,
    TargetNoPrimaryKeys,
    TargetOptionalAttributes,
    TargetRecordBeforeSchemaTest,
    TargetRecordMissingKeyProperty,
    TargetSchemaNoProperties,
    TargetSchemaUpdates,
    TargetSpecialCharsInAttributes,
)
from .templates import TestTemplate


@dataclass
class TestSuite:
    type: str
    tests: List[TestTemplate]


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
        AttributeUniquenessTest,
    ],
)


# Target Test Suites
target_tests = TestSuite(
    type="target",
    tests=[
        TargetArrayData,
        TargetCamelcaseTest,
        TargetCliPrintsTest,
        TargetDuplicateRecords,
        TargetEncodedStringData,
        TargetInvalidSchemaTest,
        TargetMultipleStateMessages,
        TargetNoPrimaryKeys,
        TargetOptionalAttributes,
        TargetRecordBeforeSchemaTest,
        TargetRecordMissingKeyProperty,
        TargetSchemaNoProperties,
        TargetSchemaUpdates,
        TargetSpecialCharsInAttributes,
    ],
)
