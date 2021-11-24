"""Generic test classes intended to be applicable to any tap or target."""

import warnings

from enum import Enum
from dateutil import parser
from typing import List, Any

from singer_sdk.tap_base import Tap


class TestTemplate:
    """
    Each Test class requires one or more of the following arguments.

    Possible Args:
        tap_class (str, optional): The class for the tap.
        tap_config (str, optional): Config to be used when initializing the tap.
        stream (obj, optional): Initialized stream object to be tested.
        stream_name (str, optional): Name of the stream to be tested.
        stream_records (list[obj]): Array of records output by the stream sync.
        attribute_name (str, optional): Name of the attribute to be tested.

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
            raise ValueError("Test must have 'name' and 'type' properties.")
        for p in self.required_args:
            setattr(self, p, kwargs[p])

    @property
    def id(self):
        raise NotImplementedError("Method not implemented.")

    def run_test(self):
        raise NotImplementedError("Method not implemented.")


class TapTestTemplate(TestTemplate):
    type = "tap"
    required_args = ["tap_class", "tap_config"]

    @property
    def id(self):
        return f"tap__{self.name}"


class StreamTestTemplate(TestTemplate):
    type = "stream"
    required_args = ["stream", "stream_name", "stream_records"]

    @property
    def id(self):
        return f"{self.stream_name}__{self.name}"


class AttributeTestTemplate(TestTemplate):
    type = "attribute"
    required_args = ["stream_records", "stream_name", "attribute_name"]

    @property
    def id(self):
        return f"{self.stream_name}__{self.attribute_name}__{self.name}"

    @property
    def non_null_attribute_values(self) -> List[Any]:
        """Helper function to extract attribute values from stream records."""
        values = [
            r[self.attribute_name]
            for r in self.stream_records
            if r.get(self.attribute_name) is not None
        ]
        if not values:
            warnings.warn(UserWarning("No records were available to test."))
        return values


class TapCLIPrintsTest(TapTestTemplate):
    "Test that the tap is able to print standard metadata."
    name = "cli_prints"

    def run_test(self):
        tap = self.tap_class(config=self.tap_config)
        tap.print_version()
        tap.print_about()
        tap.print_about(format="json")


class TapDiscoveryTest(TapTestTemplate):
    "Test that discovery mode generates a valid tap catalog."
    name = "discovery"

    def run_test(self) -> None:
        tap1 = self.tap_class(config=self.tap_config)
        tap1.run_discovery()
        catalog = tap1.catalog_dict
        # Reset and re-initialize with an input catalog
        tap2: Tap = self.tap_class(config=self.tap_config, catalog=catalog)
        assert tap2


class TapStreamConnectionTest(TapTestTemplate):
    "Test that the tap can connect to each stream."
    name = "stream_connections"

    def run_test(self) -> None:
        # Initialize with basic config
        tap = self.tap_class(config=self.tap_config)
        tap.run_connection_test()


class StreamReturnsRecordTest(StreamTestTemplate):
    "Test that a stream sync returns at least 1 record."
    name = "returns_record"

    def run_test(self):
        record_count = len(self.stream_records)
        assert record_count > 0, "No records returned in stream."


class StreamCatalogSchemaMatchesRecordTest(StreamTestTemplate):
    "Test that all attributes in the catalog schema are present in the record schema."
    name = "catalog_schema_matches_record"

    def run_test(self):
        stream_catalog_keys = set(self.stream.schema["properties"].keys())
        stream_record_keys = set().union(*(d.keys() for d in self.stream_records))
        diff = stream_catalog_keys - stream_record_keys

        assert diff == set(), f"Fields in catalog but not in record: ({diff})"


class StreamRecordSchemaMatchesCatalogTest(StreamTestTemplate):
    "Test that all attributes in the record schema are present in the catalog schema."
    name = "record_schema_matches_catalog"

    def run_test(self):
        stream_catalog_keys = set(self.stream.schema["properties"].keys())
        stream_record_keys = set().union(*(d.keys() for d in self.stream_records))
        diff = stream_record_keys - stream_catalog_keys

        assert diff == set(), f"Fields in records but not in catalog: ({diff})"


class StreamPrimaryKeysTest(StreamTestTemplate):
    "Test that all records for a stream's primary key are unique and non-null."
    name = "primary_keys"

    def run_test(self):
        primary_keys = self.stream.primary_keys
        record_ids = []
        for r in self.stream_records:
            record_ids.append((r[k] for k in primary_keys))
        count_unique_records = len(set(record_ids))
        count_records = len(self.stream_records)

        assert count_unique_records == count_records, (
            f"Length of set of records IDs ({count_unique_records})"
            f" is not equal to number of records ({count_records})."
        )
        assert all(
            all(k is not None for k in pk) for pk in record_ids
        ), "Primary keys contain some key values that are null."


class AttributeIsDateTimeTest(AttributeTestTemplate):
    "Test that a given attribute contains unique values (ignores null values)."
    name = "is_datetime"

    def run_test(self):
        for v in self.non_null_attribute_values:
            try:
                error_message = f"Unable to parse value ('{v}') with datetime parser."
                assert parser.parse(v), error_message
            except parser.ParserError as e:
                raise AssertionError(error_message) from e


class AttributeIsBooleanTest(AttributeTestTemplate):
    "Test that an attribute is of boolean datatype (or can be cast to it)."
    name = "is_boolean"

    def run_test(self):
        "Test that a given attribute does not contain any null values."
        for v in self.non_null_attribute_values:
            assert isinstance(v, bool) or str(v).lower() in [
                "true",
                "false",
            ], f"Unable to cast value ('{v}') to boolean type."


class AttributeIsObjectTest(AttributeTestTemplate):
    "Test that a given attribute is an object type."
    name = "is_object"

    def run_test(self):
        for v in self.non_null_attribute_values:
            assert isinstance(v, dict), f"Unable to cast value ('{v}') to dict type."


class AttributeIsInteger(AttributeTestTemplate):
    "Test that a given attribute can be converted to an integer type."
    name = "is_integer"

    def run_test(self):
        for v in self.non_null_attribute_values:
            assert isinstance(v, int), f"Unable to cast value ('{v}') to int type."


class AttributeIsNumberTest(AttributeTestTemplate):
    "Test that a given attribute can be converted to a floating point number type."
    name = "is_numeric"

    def run_test(self):
        for v in self.non_null_attribute_values:
            try:
                error_message = f"Unable to cast value ('{v}') to float type."
                assert isinstance(v, float) or isinstance(v, int), error_message
            except Exception as e:
                raise AssertionError(error_message) from e


class AttributeNotNullTest(AttributeTestTemplate):
    "Test that a given attribute does not contain any null values."
    name = "not_null"

    def run_test(self):
        for r in self.stream_records:
            assert (
                r.get(self.attribute_name) is not None
            ), f"Detected null records in attribute ('{self.attribute_name}')."


class AttributeUniquenessTest(AttributeTestTemplate):
    "Test that a given attribute contains unique values, ignoring nulls."
    name = "unique"

    def run_test(self):
        values = self.non_null_attribute_values
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
