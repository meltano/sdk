"""Generic test classes meant to be applicable to any tap or target."""

import warnings

from singer_sdk.tap_base import Tap

from enum import Enum
from dateutil import parser
from typing import List, Any


class TapValidationError(Exception):
    pass


class TestTemplate:
    """
    The following attributes are passed down from the TapTestRunner during
    the generation of tests.

    Possible Args:
        tap_class (str, optional): [description]. Defaults to None.
        tap_config (str, optional): [description]. Defaults to None.
        test_runner (str, optional): [description]. Defaults to None.
        stream_name (str, optional): [description]. Defaults to None.
        attribute_name (str, optional): [description]. Defaults to None.

    Raises:
        ValueError: [description]
        NotImplementedError: [description]
        NotImplementedError: [description]
    """

    name: str = None
    type: str = None
    required_args: List[str] = []

    def __init__(self, **kwargs):
        if not self.name or not self.type:
            raise ValueError("Tests must have 'name' and 'type' set.")
        for p in self.required_args:
            setattr(self, p, kwargs[p])

    @property
    def id(self):
        raise NotImplementedError("Method not implemented in template class.")

    def run_test(self):
        raise NotImplementedError("Method not implemented in template class.")


class TapTestTemplate(TestTemplate):
    type = "tap"
    required_args = ["test_runner", "tap_class", "tap_config"]

    @property
    def id(self):
        return f"tap__{self.name}"


class StreamTestTemplate(TestTemplate):
    type = "stream"
    required_args = ["test_runner", "stream_name"]

    @property
    def id(self):
        return f"{self.stream_name}__{self.name}"


class AttributeTestTemplate(TestTemplate):
    type = "attribute"
    required_args = ["test_runner", "stream_name", "attribute_name"]

    @property
    def id(self):
        return f"{self.stream_name}__{self.attribute_name}__{self.name}"

    def _extract_non_null_attribute_values(self) -> List[Any]:
        """Helper function to extract attribute values from stream records."""
        values = [
            r[self.attribute_name]
            for r in self.test_runner.records[self.stream_name]
            if r.get(self.attribute_name) is not None
        ]
        if not values:
            warnings.warn(UserWarning("No records were available to test."))
        return values


class TapCLIPrintsTest(TapTestTemplate):
    name = "cli_prints"

    def run_test(self):
        tap = self.test_runner.create_new_tap()
        tap.print_version()
        tap.print_about()
        tap.print_about(format="json")


class TapDiscoveryTest(TapTestTemplate):
    name = "discovery"

    def run_test(self) -> None:
        tap1 = self.test_runner.create_new_tap()
        tap1.run_discovery()
        catalog = tap1.catalog_dict
        # Reset and re-initialize with an input catalog
        tap2: Tap = self.test_runner.create_new_tap(catalog=catalog)
        assert tap2


class TapStreamConnectionTest(TapTestTemplate):
    name = "stream_connections"

    def run_test(self) -> None:
        # Initialize with basic config
        tap = self.test_runner.create_new_tap()
        tap.run_connection_test()


class StreamReturnsRecordTest(StreamTestTemplate):
    "The stream sync should have returned at least 1 record."
    name = "returns_record"

    def run_test(self):
        record_count = len(self.test_runner.records[self.stream_name])
        assert record_count > 0


class StreamCatalogSchemaMatchesRecordTest(StreamTestTemplate):
    "The stream's first record should have a catalog identical to that defined."
    name = "catalog_schema_matches_record"

    def run_test(self):
        stream = self.test_runner.tap.streams[self.stream_name]
        stream_catalog_keys = set(stream.schema["properties"].keys())
        stream_record_keys = set().union(
            *(d["record"].keys() for d in self.test_runner.records[self.stream_name])
        )
        diff = stream_catalog_keys - stream_record_keys

        assert diff == set(), f"Fields in catalog but not in record: ({diff})"


class StreamRecordSchemaMatchesCatalogTest(StreamTestTemplate):
    name = "record_schema_matches_catalog"

    def run_test(self):
        "The stream's first record should have a catalog identical to that defined."
        stream = self.test_runner.tap.streams[self.stream_name]
        stream_catalog_keys = set(stream.schema["properties"].keys())
        stream_record_keys = set().union(
            *(d["record"].keys() for d in self.test_runner.records[self.stream_name])
        )
        diff = stream_record_keys - stream_catalog_keys

        assert diff == set(), f"Fields in records but not in catalog: ({diff})"


class StreamPrimaryKeysTest(StreamTestTemplate):
    "Test that all records for a stream's primary key are unique and non-null."
    name = "primary_keys"

    def run_test(self):
        primary_keys = self.test_runner.tap.streams[self.stream_name].primary_keys
        record_ids = []
        for r in self.test_runner.records[self.stream_name]:
            id = (r[k] for k in primary_keys)
            record_ids.append(id)

        assert len(set(record_ids)) == len(self.test_runner.records)
        assert all(all(k is not None for k in pk) for pk in record_ids)


class AttributeIsDateTimeTest(AttributeTestTemplate):
    "Test that a given attribute contains unique values, ignoring nulls."
    name = "is_datetime"

    def run_test(self):
        values = self._extract_non_null_attribute_values()
        for v in values:
            try:
                parser.parse(v)
            except parser.ParserError as e:
                raise TapValidationError(f"Unable to parse value ('{v}') with datetime parser.") from e


class AttributeIsBooleanTest(AttributeTestTemplate):
    "Test taht a given attribute is boolean datatype."
    name = "is_boolean"

    def run_test(self):
        "Test that a given attribute does not contain any null values."
        values = self._extract_non_null_attribute_values()
        for v in values:
            try:
                bool(v)
            except ValueError as e:
                raise TapValidationError(f"Unable to cast value ('{v}') to boolean type.") from e


class AttributeIsObjectTest(AttributeTestTemplate):
    "Test that a given attribute is an object type."
    name = "is_object"

    def run_test(self):
        values = self._extract_non_null_attribute_values()
        for v in values:
            try:
                dict(v)
            except ValueError as e:
                raise TapValidationError(f"Unable to cast value ('{v}') to dict type.") from e


class AttributeIsInteger(AttributeTestTemplate):
    "Test that a given attribute can be converted to an integer type."
    name = "is_integer"

    def run_test(self):
        values = self._extract_non_null_attribute_values()
        for v in values:
            try:
                int(v)
            except ValueError as e:
                raise TapValidationError(f"Unable to cast value ('{v}') to int type.") from e


class AttributeIsNumberTest(AttributeTestTemplate):
    "Test that a given attribute can be converted to a floating point number type."
    name = "is_numeric"

    def run_test(self):
        values = self._extract_non_null_attribute_values()
        for v in values:
            try:
                float(v)
            except ValueError as e:
                raise TapValidationError(f"Unable to cast value ('{v}') to float type.") from e


class AttributeNotNullTest(AttributeTestTemplate):
    "Test that a given attribute does not contain any null values."
    name = "not_null"

    def run_test(self):
        assert all(
            r.get(self.attribute_name) is not None for r in self.test_runner.records
        ), "Detected null records in attribute."


class AttributeUniquenessTest(AttributeTestTemplate):
    "Test that a given attribute contains unique values, ignoring nulls."
    name = "unique"

    def run_test(self):
        values = self._extract_non_null_attribute_values()
        assert len(set(values)) == len(
            values
        ), f"Attribute ({self.attribute_name}) is not unique."


class TapTests(Enum):
    cli_prints = TapCLIPrintsTest
    discovery = TapDiscoveryTest
    stream_connection = TapStreamConnectionTest


class StreamTests(Enum):
    catalog_schema_matches_records = StreamCatalogSchemaMatchesRecordTest
    record_schema_matches_catalog = StreamRecordSchemaMatchesCatalogTest
    returns_records = StreamReturnsRecordTest
    primary_keys = StreamPrimaryKeysTest


class AttributeTests(Enum):
    is_boolean = AttributeIsBooleanTest
    is_datetime = AttributeIsDateTimeTest
    is_integer = AttributeIsInteger
    is_number = AttributeIsNumberTest
    is_object = AttributeIsObjectTest
    not_null = AttributeNotNullTest
    unique = AttributeUniquenessTest
