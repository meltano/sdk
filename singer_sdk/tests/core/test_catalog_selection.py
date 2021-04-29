"""Test catalog selection features."""

import logging
import pytest

import singer

from singer_sdk.helpers._catalog import get_selected_schema, is_property_selected
from singer_sdk.typing import PropertiesList, Property, StringType, ObjectType


@pytest.fixture
def record():
    return {
        "col_a": {
            "col_a_1": "something",
            "col_a_2": "else",
        },
        "col_b": {
            "col_b_1": "the answer",
            "col_b_2": "42",
        },
    }


@pytest.fixture
def stream_name():
    return "test_stream_a"


@pytest.fixture
def schema():
    return PropertiesList(
        Property(
            "col_a",
            ObjectType(
                Property("col_a_1", StringType),
                Property("col_a_2", StringType),
            ),
        ),
        Property(
            "col_b",
            ObjectType(
                Property("col_b_1", StringType),
                Property("col_b_2", StringType),
            ),
        ),
    ).to_dict()


@pytest.fixture
def selection_metadata():
    return [
        {
            "breadcrumb": (),
            "metadata": {
                "selected": True,
            },
        },
        {
            "breadcrumb": ("properties", "col_a", "col_a_2"),
            "metadata": {
                "selected": False,  # Should not be overridden by parent
            },
        },
        {
            "breadcrumb": ("properties", "col_b"),
            "metadata": {
                "selected": False,
            },
        },
        {
            "breadcrumb": ("properties", "col_b", "col_b_1"),
            "metadata": {
                "selected": True,  # Should be overridden by parent
            },
        },
    ]


@pytest.fixture
def catalog_entry_obj(schema, stream_name, selection_metadata) -> singer.CatalogEntry:
    return singer.CatalogEntry(
        tap_stream_id=stream_name,
        stream=stream_name,
        schema=singer.Schema.from_dict(schema),
        metadata=selection_metadata,
    )


@pytest.fixture
def catalog_entry_dict(catalog_entry_obj) -> dict:
    return catalog_entry_obj.to_dict()


@pytest.fixture
def catalog(catalog_entry_obj):
    return singer.Catalog(streams=[catalog_entry_obj]).to_dict()


@pytest.fixture
def selection_test_cases():
    return [
        ((), True),
        (("properties", "col_a"), True),
        (("properties", "col_a", "col_a_1"), True),
        (("properties", "col_a", "col_a_2"), False),
        (("properties", "col_b"), False),
        (("properties", "col_b", "col_b_1"), False),
        (("properties", "col_b", "col_b_2"), False),
    ]


def test_schema_selection(catalog, stream_name):
    """Test that schema selection rules are correctly applied to SCHEMA messages."""
    selected_schema = get_selected_schema(catalog, stream_name, logging.getLogger())
    # selected_schema["properties"]["required"] = []
    assert (
        selected_schema["properties"]
        == PropertiesList(
            Property(
                "col_a",
                ObjectType(
                    Property("col_a_1", StringType),
                ),
            )
        ).to_dict()["properties"]
    )


def test_record_selection(record, catalog, stream_name, selection_test_cases, caplog):
    """Test that record selection rules are correctly applied to SCHEMA messages."""
    caplog.set_level(logging.DEBUG)
    for bookmark, expected in selection_test_cases:
        assert (
            is_property_selected(
                catalog,
                stream_name,
                breadcrumb=bookmark,
                logger=logging.getLogger(),
            )
            == expected
        ), f"bookmark {bookmark} was expected to be selected={expected}"
