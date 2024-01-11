"""Test the custom type validator."""

from __future__ import annotations

import pytest
from typing_extensions import override

from singer_sdk.sinks.core import BaseJSONSchemaValidator, InvalidJSONSchema, Sink
from singer_sdk.target_base import Target


@pytest.fixture
def test_schema_invalid():
    """Return a test schema with an invalid type."""

    return {
        "type": "object",
        "properties": {
            "datetime_col": {"type": "ssttrriinngg", "format": "date-time"},
        },
    }


@pytest.fixture
def target():
    """Return a target object."""

    class CustomTarget(Target):
        name = "test_target"

    return CustomTarget()


def test_default_schema_type_checks(target, test_schema_invalid):
    """Test type checks on _validator initialization."""

    class CustomSink(Sink):
        """Custom sink class."""

        @override
        def process_batch(self, context: dict) -> None:
            pass

        @override
        def process_record(self, record: dict, context: dict) -> None:
            pass

    with pytest.raises(
        InvalidJSONSchema,
        match=r"Schema Validation Error: 'ssttrriinngg' is not valid under any",
    ):
        CustomSink(target, "test_stream", test_schema_invalid, None)


def test_disable_schema_type_checks_returning_none(target, test_schema_invalid):
    """Test type checks on _validator initialization."""

    class CustomSink(Sink):
        """Custom sink class."""

        @override
        def get_validator(self) -> BaseJSONSchemaValidator | None:
            """Get a record validator for this sink.

            Override this method to use a custom format validator
            or disable jsonschema validator, by returning `None`.

            Returns:
                An instance of a subclass of ``BaseJSONSchemaValidator``.
            """
            return None

        @override
        def process_batch(self, context: dict) -> None:
            pass

        @override
        def process_record(self, record: dict, context: dict) -> None:
            pass

    CustomSink(target, "test_stream", test_schema_invalid, None)


def test_disable_schema_type_checks_setting_false(target, test_schema_invalid):
    """Test type checks on _validator initialization."""

    class CustomSink(Sink):
        """Custom sink class."""

        validate_schema = False

        @override
        def process_batch(self, context: dict) -> None:
            pass

        @override
        def process_record(self, record: dict, context: dict) -> None:
            pass

    CustomSink(target, "test_stream", test_schema_invalid, None)
