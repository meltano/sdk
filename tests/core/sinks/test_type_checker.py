"""Test the custom type validator."""

from __future__ import annotations

import fastjsonschema
import pytest

from singer_sdk.sinks.core import Sink
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

        def process_batch(self, context: dict) -> None:
            pass

        def process_record(self, record: dict, context: dict) -> None:
            pass

    with pytest.raises(
        fastjsonschema.exceptions.JsonSchemaDefinitionException,
        match=r"Unknown type: 'ssttrriinngg'",
    ):
        CustomSink(target, "test_stream", test_schema_invalid, None)
