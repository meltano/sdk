"""Standard Target tests."""

from __future__ import annotations

import pytest

from singer_sdk.exceptions import (
    MissingKeyPropertiesError,
    RecordsWithoutSchemaException,
)

from .templates import TargetFileTestTemplate, TargetTestTemplate


class TargetArrayData(TargetFileTestTemplate):
    """Test Target handles array data."""

    name = "array_data"


class TargetCamelcaseComplexSchema(TargetFileTestTemplate):
    """Test Target handles CaMeLcAsE record key and attributes, nested."""

    name = "camelcase_complex_schema"


class TargetCamelcaseTest(TargetFileTestTemplate):
    """Test Target handles CaMeLcAsE record key and attributes."""

    name = "camelcase"


class TargetCliPrintsTest(TargetTestTemplate):
    """Test Target correctly prints version and about information."""

    name = "cli_prints"

    def test(self) -> None:
        """Run test."""
        self.target.print_version()
        self.target.print_about()
        self.target.print_about(output_format="json")


class TargetDuplicateRecords(TargetFileTestTemplate):
    """Test Target handles duplicate records."""

    name = "duplicate_records"


class TargetEncodedStringData(TargetFileTestTemplate):
    """Test Target handles encoded string data."""

    name = "encoded_string_data"


class TargetInvalidSchemaTest(TargetFileTestTemplate):
    """Test Target handles an invalid schema message."""

    name = "invalid_schema"

    def test(self) -> None:
        """Run test."""
        # TODO: the SDK should raise a better error than Exception in this case
        # https://github.com/meltano/sdk/issues/1755
        with pytest.raises(Exception):  # noqa: PT011, B017
            super().test()


class TargetMultipleStateMessages(TargetFileTestTemplate):
    """Test Target correctly relays multiple received State messages (checkpoints)."""

    name = "multiple_state_messages"

    def test(self) -> None:
        """Run test."""
        self.runner.sync_all()
        state_messages = self.runner.state_messages
        assert state_messages == [
            '{"test_multiple_state_messages_a": 1, "test_multiple_state_messages_b": 0}',  # noqa: E501
            '{"test_multiple_state_messages_a": 3, "test_multiple_state_messages_b": 2}',  # noqa: E501
            '{"test_multiple_state_messages_a": 5, "test_multiple_state_messages_b": 6}',  # noqa: E501
        ]


class TargetNoPrimaryKeys(TargetFileTestTemplate):
    """Test Target handles records without primary keys."""

    name = "no_primary_keys"


class TargetOptionalAttributes(TargetFileTestTemplate):
    """Test Target handles optional record attributes."""

    name = "optional_attributes"


class TargetRecordBeforeSchemaTest(TargetFileTestTemplate):
    """Test Target handles records arriving before schema."""

    name = "record_before_schema"

    def test(self) -> None:
        """Run test."""
        with pytest.raises(RecordsWithoutSchemaException):
            super().test()


class TargetRecordMissingKeyProperty(TargetFileTestTemplate):
    """Test Target handles record missing key property."""

    name = "record_missing_key_property"

    def test(self) -> None:
        """Run test."""
        with pytest.raises(MissingKeyPropertiesError):
            super().test()


class TargetRecordMissingRequiredProperty(TargetFileTestTemplate):
    """Test Target handles record missing required property."""

    name = "record_missing_required_property"


class TargetSchemaNoProperties(TargetFileTestTemplate):
    """Test Target handles schema with no properties."""

    name = "schema_no_properties"


class TargetSchemaUpdates(TargetFileTestTemplate):
    """Test Target handles schema updates."""

    name = "schema_updates"


class TargetPrimaryKeyUpdates(TargetFileTestTemplate):
    """Test Target handles Primary Key updates."""

    name = "pk_updates"


class TargetSpecialCharsInAttributes(TargetFileTestTemplate):
    """Test Target handles special chars in attributes."""

    name = "special_chars_in_attributes"


class TargetRecordMissingOptionalFields(TargetFileTestTemplate):
    """Test Target handles record missing optional fields."""

    name = "record_missing_fields"
