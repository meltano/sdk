"""Test catalog selection features."""

import logging
from typing import cast
from copy import deepcopy

import singer

import pytest

from singer_sdk.helpers._catalog import (
    get_selected_schema,
    is_property_selected,
    pop_deselected_record_properties,
)
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
        "col_d": "by-default",
        "col_e": "automatic",
    }


@pytest.fixture
def record_selected():
    return {
        "col_a": {
            "col_a_1": "something",
        },
        "col_d": "by-default",
        "col_e": "automatic",
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
        Property(
            "col_c",
            StringType,
        ),
        Property(
            "col_d",
            StringType,
        ),
        Property(
            "col_e",
            StringType,
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
            "breadcrumb": ("properties", "col_a", "properties", "col_a_2"),
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
            "breadcrumb": ("properties", "col_b", "properties", "col_b_1"),
            "metadata": {
                "selected": True,  # Should be overridden by parent
            },
        },
        {
            "breadcrumb": ("properties", "col_c"),
            "metadata": {
                "inclusion": "unsupported",
                "selected": True,  # Should be overridden by 'inclusion'
            },
        },
        {
            "breadcrumb": ("properties", "col_d"),
            "metadata": {"selected-by-default": True},
        },
        {
            "breadcrumb": ("properties", "col_e"),
            "metadata": {
                "inclusion": "automatic",
                "selected": False,  # Should be overridden by 'inclusion'
            },
        },
        {
            "breadcrumb": ("properties", "missing"),
            "metadata": {"selected": True},
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
def catalog_entry_dict(catalog_entry_obj: singer.CatalogEntry) -> dict:
    return cast(dict, catalog_entry_obj.to_dict())


@pytest.fixture
def catalog(catalog_entry_obj):
    return singer.Catalog(streams=[catalog_entry_obj]).to_dict()


@pytest.fixture
def selection_test_cases():
    return [
        ((), True),
        (("properties", "col_a"), True),
        (("properties", "col_a", "properties", "col_a_1"), True),
        (("properties", "col_a", "properties", "col_a_2"), False),
        (("properties", "col_b"), False),
        (("properties", "col_b", "properties", "col_b_1"), False),
        (("properties", "col_b", "properties", "col_b_2"), False),
        (("properties", "col_c"), False),
        (("properties", "col_d"), True),
        (("properties", "col_e"), True),
    ]


def test_schema_selection(catalog_entry_obj, stream_name):
    """Test that schema selection rules are correctly applied to SCHEMA messages."""
    selected_schema = get_selected_schema(
        stream_name,
        catalog_entry_obj.schema.to_dict(),
        catalog_entry_obj.metadata,
        logging.getLogger(),
    )
    # selected_schema["properties"]["required"] = []
    assert (
        selected_schema["properties"]
        == PropertiesList(
            Property(
                "col_a",
                ObjectType(
                    Property("col_a_1", StringType),
                ),
            ),
            Property("col_d", StringType),
            Property("col_e", StringType),
        ).to_dict()["properties"]
    )


def test_record_selection(
    record, catalog_entry_obj, stream_name, selection_test_cases, caplog
):
    """Test that record selection rules are correctly applied to SCHEMA messages."""
    caplog.set_level(logging.DEBUG)
    for bookmark, expected in selection_test_cases:
        assert (
            is_property_selected(
                stream_name,
                catalog_entry_obj.metadata,
                breadcrumb=bookmark,
                logger=logging.getLogger(),
            )
            == expected
        ), f"bookmark {bookmark} was expected to be selected={expected}"


def test_record_property_pop(
    record, record_selected, catalog_entry_obj, stream_name, caplog
):
    """Test that properties are correctly selected/deselected on a record."""
    caplog.set_level(logging.DEBUG)
    record_pop = deepcopy(record)
    pop_deselected_record_properties(
        record=record_pop,
        schema=catalog_entry_obj.schema,
        metadata=catalog_entry_obj.metadata,
        stream_name=stream_name,
        logger=logging.getLogger(),
        breadcrumb=(),
    )

    assert (
        record_pop == record_selected
    ), f"Expected record={record_selected}, got {record_pop}"


def test_no_selection_metadata():
    """Test an exception is raised when no selection status is detected."""
    metadata = [
        {
            "breadcrumb": [],
            "metadata": {
                "selected": True,
                "replication-method": "FULL_TABLE",
            },
        },
        {
            "breadcrumb": ("properties", "field_one"),
            "metadata": {
                # Some metadata, but not for selection
                "some-other-metadata": "whatever"
            },
        },
    ]

    schema = {"type": "object", "properties": {"field_one": {"type": "string"}}}

    with pytest.raises(ValueError, match="Could not detect selection status .*"):
        is_property_selected(
            stream_name="test",
            metadata=metadata,
            breadcrumb=("properties", "field_one"),
            logger=logging.getLogger(),
        )
