"""Standard Tap Tests."""

from __future__ import annotations

import typing as t
import warnings

import pytest
from jsonschema import validators
from jsonschema.exceptions import SchemaError

import singer_sdk.helpers._typing as th
from singer_sdk import Tap
from singer_sdk.helpers._compat import datetime_fromisoformat
from singer_sdk.typing import DEFAULT_JSONSCHEMA_VALIDATOR

from .templates import AttributeTestTemplate, StreamTestTemplate, TapTestTemplate

if t.TYPE_CHECKING:
    from singer_sdk.streams.core import Stream


class TapCLIPrintsTest(TapTestTemplate):
    """Test that the tap is able to print standard metadata."""

    name = "cli_prints"

    def test(self) -> None:
        """Run test."""
        self.tap.print_version()
        self.tap.print_about()
        self.tap.print_about(output_format="json")


class TapDiscoveryTest(TapTestTemplate):
    """Test that discovery mode generates a valid tap catalog."""

    name = "discovery"

    def test(self) -> None:
        """Run test."""
        tap1 = self.tap
        tap1.run_discovery()
        catalog = tap1.catalog_dict
        # Reset and re-initialize with discovered catalog
        kwargs = {k: v for k, v in self.runner.default_kwargs.items() if k != "catalog"}
        tap2: Tap = t.cast("type[Tap]", self.runner.singer_class)(
            config=self.runner.config,
            catalog=catalog,
            **kwargs,
        )
        assert tap2  # type: ignore[truthy-bool]


class TapStreamConnectionTest(TapTestTemplate):
    """Test that the tap can connect to each stream."""

    name = "stream_connections"

    def test(self) -> None:
        """Run test."""
        self.tap.run_connection_test()


class TapValidFinalStateTest(TapTestTemplate):
    """Test that the final state is a valid catalog."""

    name = "valid_final_state"
    message = "Final state has in-progress markers."

    def test(self) -> None:
        """Run test."""
        final_state = self.runner.state_messages[-1]
        assert "progress_markers" not in final_state, self.message


class StreamSchemaIsValidTest(StreamTestTemplate):
    """Test that a stream's schema is valid."""

    name = "schema_is_valid"

    def test(self) -> None:
        """Run test.

        Raises:
            AssertionError: if schema is not valid.
        """
        schema = self.stream.schema
        default = DEFAULT_JSONSCHEMA_VALIDATOR
        validator = validators.validator_for(schema, default=default)

        try:
            validator.check_schema(schema)
        except SchemaError as e:  # pragma: no cover
            msg = f"Schema is not valid: {e}"
            raise AssertionError(msg) from e


class StreamReturnsRecordTest(StreamTestTemplate):
    """Test that a stream sync returns at least 1 record."""

    name = "returns_record"

    def test(self) -> None:
        """Run test."""
        if self.ignore_no_records:
            pytest.skip(
                "Skipping test because no records were returned in "
                f"stream '{self.stream.name}'",
            )

        record_count = len(self.stream_records)
        assert record_count > 0, f"No records returned in stream '{self.stream.name}'."


class StreamCatalogSchemaMatchesRecordTest(StreamTestTemplate):
    """Test all attributes in the catalog schema are present in the record schema."""

    name = "transformed_catalog_schema_matches_record"

    def test(self) -> None:
        """Run test."""
        if not self.stream_records:
            return

        stream_transformed_keys: set[str] = set(
            self.stream.stream_maps[-1].transformed_schema["properties"].keys(),
        )
        stream_record_keys: set[str] = set().union(
            *(d.keys() for d in self.stream_records)
        )
        diff = stream_transformed_keys - stream_record_keys
        if diff:
            warnings.warn(
                UserWarning(
                    f"Fields in transformed catalog but not in records: ({diff})",
                ),
                stacklevel=2,
            )


class StreamRecordSchemaMatchesCatalogTest(StreamTestTemplate):
    """Test all attributes in the record schema are present in the catalog schema."""

    name = "record_schema_matches_transformed_catalog"

    def test(self) -> None:
        """Run test."""
        stream_transformed_keys = set(
            self.stream.stream_maps[-1].transformed_schema["properties"].keys(),
        )
        stream_record_keys = set().union(*(d.keys() for d in self.stream_records))
        diff = stream_record_keys - stream_transformed_keys
        assert not diff, f"Fields in records but not in transformed catalog: ({diff})"


class StreamRecordMatchesStreamSchema(StreamTestTemplate):
    """Test all attributes in the record schema are present in the catalog schema."""

    name = "record_matches_stream_schema"

    def test(self) -> None:
        """Run test."""
        schema = self.stream.schema
        default = DEFAULT_JSONSCHEMA_VALIDATOR
        validator = validators.validator_for(schema, default=default)(schema)
        validator.format_checker = default.FORMAT_CHECKER

        for record in self.stream_records:
            errors = list(validator.iter_errors(record))
            error_messages = "\n".join(
                [
                    f"{e.message} (path: {'.'.join(str(p) for p in e.path)})"
                    for e in errors
                    if e.path
                ],
            )
            assert not errors, f"Record does not match stream schema: {error_messages}"


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
            msg = f"Record missing primary key: {e!s}"
            raise AssertionError(msg) from e
        count_unique_records = len(set(record_ids))
        count_records = len(self.stream_records)
        assert count_unique_records == count_records, (
            f"Length of set of records IDs ({count_unique_records})"
            f" is not equal to number of records ({count_records})."
        )
        assert all(all(k is not None for k in pk) for pk in record_ids), (
            "Primary keys contain some key values that are null."
        )


class AttributeIsDateTimeTest(AttributeTestTemplate):
    """Test a given attribute contains unique values (ignores null values)."""

    name = "is_datetime"

    def test(self) -> None:
        """Run test.

        Raises:
            AssertionError: if value cannot be parsed as a datetime.
        """
        try:
            for v in self.non_null_attribute_values:
                error_message = f"Unable to parse value ('{v}') with datetime parser."
                assert datetime_fromisoformat(v), error_message  # type: ignore[truthy-bool]
        except ValueError as e:
            raise AssertionError(error_message) from e

    @classmethod
    def evaluate(
        cls,
        stream: Stream,  # noqa: ARG003
        property_name: str,  # noqa: ARG003
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
        stream: Stream,  # noqa: ARG003
        property_name: str,  # noqa: ARG003
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
        stream: Stream,  # noqa: ARG003
        property_name: str,  # noqa: ARG003
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
                int(v),
                int,
            ), f"Unable to cast value ('{v}') to int type."

    @classmethod
    def evaluate(
        cls,
        stream: Stream,  # noqa: ARG003
        property_name: str,  # noqa: ARG003
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
            error_message = f"Unable to cast value ('{v}') to float type."
            if not isinstance(v, (float, int)):
                raise AssertionError(error_message)  # noqa: TRY004

    @classmethod
    def evaluate(
        cls,
        stream: Stream,  # noqa: ARG003
        property_name: str,  # noqa: ARG003
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
            assert r.get(self.attribute_name) is not None, (
                f"Detected null values for attribute ('{self.attribute_name}')."
            )

    @classmethod
    def evaluate(
        cls,
        stream: Stream,  # noqa: ARG003
        property_name: str,  # noqa: ARG003
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
