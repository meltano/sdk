"""Test catalog selection features."""

from singer_sdk.helpers._catalog import get_selected_schema
from singer_sdk.typing import PropertiesList, Property, StringType, ObjectType


def test_schema_selection():
    """Test that schema selection rules are correctly applied to SCHEMA messages."""
    schema = PropertiesList(
        Property(
            "col_a",
            ObjectType(
                Property("col_a_1", StringType),
                Property("col_a_2", StringType),
            ),
        )
    ).to_dict()
    catalog_entry = {}  # TODO: construct a filtering catalog dict, excluding col_a_2
    selected_schema = get_selected_schema(schema, catalog_entry)
    assert (
        selected_schema
        == PropertiesList(
            Property(
                "col_a",
                ObjectType(
                    Property("col_a_1", StringType),
                ),
            )
        ).to_dict()
    )
