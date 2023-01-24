"""Standard Tap Tests."""

import warnings
from typing import Type, cast

from dateutil import parser

import singer_sdk.helpers._typing as th
from singer_sdk import Tap
from singer_sdk.streams.core import Stream

from .templates import AttributeTestTemplate, StreamTestTemplate, TapTestTemplate


class TapCLIPrintsTest(TapTestTemplate):
    """Test that the tap is able to print standard metadata."""

    name = "cli_prints"

    def test(self) -> None:
        """Run test."""
        self.tap.print_version()
        self.tap.print_about()
        self.tap.print_about(format="json")


class TapDiscoveryTest(TapTestTemplate):
    """Test that discovery mode generates a valid tap catalog."""

    name = "discovery"

    def test(self) -> None:
        """Run test."""
        tap1 = self.tap
        tap1.run_discovery()
        catalog = tap1.catalog_dict
        # Reset and re-initialize with an input catalog
        tap2: Tap = cast(Type[Tap], self.runner.singer_class)(
            config=self.runner.config, catalog=catalog, **self.runner.default_kwargs
        )
        assert tap2


class TapStreamConnectionTest(TapTestTemplate):
    """Test that the tap can connect to each stream."""

    name = "stream_connections"

    def test(self) -> None:
        """Run test."""
        self.tap.run_connection_test()


class StreamReturnsRecordTest(StreamTestTemplate):
    """Test that a stream sync returns at least 1 record."""

    name = "returns_record"

    def test(self) -> None:
        """Run test."""
        no_records_message = f"No records returned in stream '{self.stream.name}'."
        if (
            self.config.ignore_no_records
            or self.stream.name in self.config.ignore_no_records_for_streams
        ):
            # only warn if this or all streams are set to ignore no records
            warnings.warn(UserWarning(no_records_message))
        else:
            record_count = len(self.stream_records)
            assert record_count > 0, no_records_message


class StreamCatalogSchemaMatchesRecordTest(StreamTestTemplate):
    """Test all attributes in the catalog schema are present in the record schema."""

    name = "catalog_schema_matches_record"

    def test(self) -> None:
        """Run test."""
        stream_catalog_keys = set(self.stream.schema["properties"].keys())
        stream_record_keys = set().union(*(d.keys() for d in self.stream_records))
        diff = stream_catalog_keys - stream_record_keys
        if diff:
            warnings.warn(
                UserWarning(f"Fields in catalog but not in records: ({diff})")
            )


class StreamRecordSchemaMatchesCatalogTest(StreamTestTemplate):
    """Test all attributes in the record schema are present in the catalog schema."""

    name = "record_schema_matches_catalog"

    def test(self) -> None:
        """Run test."""
        stream_catalog_keys = set(self.stream.schema["properties"].keys())
        stream_record_keys = set().union(*(d.keys() for d in self.stream_records))
        diff = stream_record_keys - stream_catalog_keys
        assert not diff, f"Fields in records but not in catalog: ({diff})"


class StreamPrimaryKeysTest(StreamTestTemplate):
    """Test all records for a stream's primary key are unique and non-null."""

    name = "primary_keys"

    def test(self) -> None:
        """Run test.

        Raises:
            AssertionError: if record is missing primary key.
        """
        primary_keys = self.stream.primary_keys
        try:
            record_ids = [
                (r[k] for k in primary_keys or []) for r in self.stream_records
            ]
        except KeyError as e:
            raise AssertionError(f"Record missing primary key: {str(e)}")
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
    """Test a given attribute contains unique values (ignores null values)."""

    name = "is_datetime"

    def test(self) -> None:
        """Run test.

        Raises:
            AssertionError: if value cannot be parsed as a datetime.
        """
        for v in self.non_null_attribute_values:
            try:
                error_message = f"Unable to parse value ('{v}') with datetime parser."
                assert parser.parse(v), error_message
            except parser.ParserError as e:
                raise AssertionError(error_message) from e

    @classmethod
    def evaluate(
        cls,
        stream: Stream,
        property_name: str,
        property_schema: dict,
    ) -> bool:
        """Determine if this attribute test is applicable to the given property.

        Args:
            stream: Parent Stream of given attribute.
            property_name: Name of given attribute.
            property_schema: JSON Schema of given property, in dict form.

        Returns:
            True if this test is applicable, False if not.
        """
        return bool(th.is_date_or_datetime_type(property_schema))


class AttributeIsBooleanTest(AttributeTestTemplate):
    """Test an attribute is of boolean datatype (or can be cast to it)."""

    name = "is_boolean"

    def test(self) -> None:
        """Run test."""
        for v in self.non_null_attribute_values:
            assert isinstance(v, bool) or str(v).lower() in {
                "true",
                "false",
            }, f"Unable to cast value ('{v}') to boolean type."

    @classmethod
    def evaluate(
        cls,
        stream: Stream,
        property_name: str,
        property_schema: dict,
    ) -> bool:
        """Determine if this attribute test is applicable to the given property.

        Args:
            stream: Parent Stream of given attribute.
            property_name: Name of given attribute.
            property_schema: JSON Schema of given property, in dict form.

        Returns:
            True if this test is applicable, False if not.
        """
        return bool(th.is_boolean_type(property_schema))


class AttributeIsObjectTest(AttributeTestTemplate):
    """Test that a given attribute is an object type."""

    name = "is_object"

    def test(self) -> None:
        """Run test."""
        for v in self.non_null_attribute_values:
            assert isinstance(v, dict), f"Unable to cast value ('{v}') to dict type."

    @classmethod
    def evaluate(
        cls,
        stream: Stream,
        property_name: str,
        property_schema: dict,
    ) -> bool:
        """Determine if this attribute test is applicable to the given property.

        Args:
            stream: Parent Stream of given attribute.
            property_name: Name of given attribute.
            property_schema: JSON Schema of given property, in dict form.

        Returns:
            True if this test is applicable, False if not.
        """
        return bool(th.is_object_type(property_schema))


class AttributeIsIntegerTest(AttributeTestTemplate):
    """Test that a given attribute can be converted to an integer type."""

    name = "is_integer"

    def test(self) -> None:
        """Run test."""
        for v in self.non_null_attribute_values:
            assert isinstance(v, int) or isinstance(
                int(v), int
            ), f"Unable to cast value ('{v}') to int type."

    @classmethod
    def evaluate(
        cls,
        stream: Stream,
        property_name: str,
        property_schema: dict,
    ) -> bool:
        """Determine if this attribute test is applicable to the given property.

        Args:
            stream: Parent Stream of given attribute.
            property_name: Name of given attribute.
            property_schema: JSON Schema of given property, in dict form.

        Returns:
            True if this test is applicable, False if not.
        """
        return bool(th.is_integer_type(property_schema))


class AttributeIsNumberTest(AttributeTestTemplate):
    """Test that a given attribute can be converted to a floating point number type."""

    name = "is_numeric"

    def test(self) -> None:
        """Run test.

        Raises:
            AssertionError: if value cannot be cast to float type.
        """
        for v in self.non_null_attribute_values:
            try:
                error_message = f"Unable to cast value ('{v}') to float type."
                assert isinstance(v, (float, int)), error_message
            except Exception as e:
                raise AssertionError(error_message) from e

    @classmethod
    def evaluate(
        cls,
        stream: Stream,
        property_name: str,
        property_schema: dict,
    ) -> bool:
        """Determine if this attribute test is applicable to the given property.

        Args:
            stream: Parent Stream of given attribute.
            property_name: Name of given attribute.
            property_schema: JSON Schema of given property, in dict form.

        Returns:
            True if this test is applicable, False if not.
        """
        return bool(th.is_number_type(property_schema))


class AttributeNotNullTest(AttributeTestTemplate):
    """Test that a given attribute does not contain any null values."""

    name = "not_null"

    def test(self) -> None:
        """Run test."""
        for r in self.stream_records:
            assert (
                r.get(self.attribute_name) is not None
            ), f"Detected null values for attribute ('{self.attribute_name}')."

    @classmethod
    def evaluate(
        cls,
        stream: Stream,
        property_name: str,
        property_schema: dict,
    ) -> bool:
        """Determine if this attribute test is applicable to the given property.

        Args:
            stream: Parent Stream of given attribute.
            property_name: Name of given attribute.
            property_schema: JSON Schema of given property, in dict form.

        Returns:
            True if this test is applicable, False if not.
        """
        return not bool(th.is_null_type(property_schema))
