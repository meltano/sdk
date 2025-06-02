"""Test catalog selection features."""

from __future__ import annotations

import logging
from copy import deepcopy

import pytest

import singer_sdk._singerlib as singer
from singer_sdk.helpers._catalog import (
    deselect_all_streams,
    get_selected_schema,
    pop_deselected_record_properties,
    set_catalog_stream_selected,
)
from singer_sdk.typing import ObjectType, PropertiesList, Property, StringType


@pytest.fixture
def record():
    return {
        "col_a": {
            "col_a_1": "something",
            "col_a_2": "else",
            "col_a_3": "altogether",
        },
        "col_b": {
            "col_b_1": "the answer",
            "col_b_2": "42",
        },
        "col_d": "by-default",
        "col_e": "automatic",
        "col_f": "available",
    }


@pytest.fixture
def record_selected():
    return {
        "col_a": {
            "col_a_1": "something",
            "col_a_3": "altogether",
        },
        "col_d": "by-default",
        "col_e": "automatic",
        "col_f": "available",
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
                Property("col_a_1", StringType, required=True),
                Property("col_a_2", StringType),
                Property("col_a_3", StringType),
            ),
            required=True,
        ),
        Property(
            "col_b",
            ObjectType(
                Property("col_b_1", StringType),
                Property("col_b_2", StringType),
            ),
            required=True,
        ),
        Property(
            "col_c",
            StringType,
            required=True,
        ),
        Property(
            "col_d",
            StringType,
            required=True,
        ),
        Property(
            "col_e",
            StringType,
        ),
        Property(
            "col_f",
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
            "breadcrumb": ("properties", "col_a", "properties", "col_a_3"),
            "metadata": {},  # No metadata means parent selection is used
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
            "breadcrumb": ("properties", "col_f"),
            "metadata": {"inclusion": "available"},
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
        metadata=singer.MetadataMapping.from_iterable(selection_metadata),
    )


@pytest.fixture
def catalog_entry_dict(catalog_entry_obj: singer.CatalogEntry) -> dict:
    return catalog_entry_obj.to_dict()


@pytest.fixture
def mask(catalog_entry_obj: singer.CatalogEntry) -> singer.SelectionMask:
    return catalog_entry_obj.metadata.resolve_selection()


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
        (("properties", "col_a", "properties", "col_a_3"), True),
        (("properties", "col_b"), False),
        (("properties", "col_b", "properties", "col_b_1"), False),
        (("properties", "col_b", "properties", "col_b_2"), False),
        (("properties", "col_c"), False),
        (("properties", "col_d"), True),
        (("properties", "col_e"), True),
        (("properties", "col_f"), True),
    ]


def test_schema_selection(
    schema: dict,
    mask: singer.SelectionMask,
    stream_name: str,
):
    """Test that schema selection rules are correctly applied to SCHEMA messages."""
    selected_schema = get_selected_schema(stream_name, schema, mask)
    assert (
        selected_schema
        == PropertiesList(
            Property(
                "col_a",
                ObjectType(
                    Property("col_a_1", StringType, required=True),
                    Property("col_a_3", StringType),
                ),
                required=True,
            ),
            Property("col_d", StringType, required=True),
            Property("col_e", StringType),
            Property("col_f", StringType),
        ).to_dict()
    )


def test_record_selection(
    catalog_entry_obj: singer.CatalogEntry,
    selection_test_cases,
    caplog,
):
    """Test that record selection rules are correctly applied to SCHEMA messages."""
    caplog.set_level(logging.DEBUG)
    for bookmark, expected in selection_test_cases:
        assert (
            catalog_entry_obj.metadata._breadcrumb_is_selected(bookmark) == expected
        ), f"bookmark {bookmark} was expected to be selected={expected}"


def test_record_property_pop(
    record: dict,
    record_selected: dict,
    schema: dict,
    mask: singer.SelectionMask,
    caplog,
):
    """Test that properties are correctly selected/deselected on a record."""
    caplog.set_level(logging.DEBUG)
    record_pop = deepcopy(record)
    pop_deselected_record_properties(
        record=record_pop,
        schema=schema,
        mask=mask,
        breadcrumb=(),
    )

    assert record_pop == record_selected, (
        f"Expected record={record_selected}, got {record_pop}"
    )


def test_deselect_all_streams():
    """Test that deselect_all_streams sets all streams to not selected."""
    catalog = singer.Catalog.from_dict(
        {
            "streams": [
                {
                    "tap_stream_id": "test_stream_a",
                    "stream": "test_stream_a",
                    "schema": {
                        "type": "object",
                        "properties": {
                            "col_a": {
                                "type": "string",
                            },
                        },
                    },
                    "metadata": [
                        {
                            "breadcrumb": (),
                            "metadata": {"selected": True},
                        },
                    ],
                },
                {
                    "tap_stream_id": "test_stream_b",
                    "stream": "test_stream_b",
                    "schema": {
                        "type": "object",
                        "properties": {
                            "col_a": {
                                "type": "string",
                            },
                        },
                    },
                    "metadata": [
                        {
                            "breadcrumb": (),
                            "metadata": {"selected": True},
                        },
                    ],
                },
            ],
        }
    )

    # Stream is selected
    assert catalog["test_stream_a"].metadata.root.selected is True
    assert catalog["test_stream_b"].metadata.root.selected is True

    deselect_all_streams(catalog)
    # After deselection, should be False
    assert catalog["test_stream_a"].metadata.root.selected is False
    assert catalog["test_stream_b"].metadata.root.selected is False


def test_set_catalog_stream_selected(catalog_entry_obj: singer.CatalogEntry):
    """Test set_catalog_stream_selected for stream and property selection."""
    catalog = singer.Catalog({catalog_entry_obj.tap_stream_id: catalog_entry_obj})
    stream_name = catalog_entry_obj.tap_stream_id

    # Set stream to not selected
    set_catalog_stream_selected(catalog, stream_name, selected=False)
    assert catalog_entry_obj.metadata.root.selected is False
    # Set stream to selected
    set_catalog_stream_selected(catalog, stream_name, selected=True)
    assert catalog_entry_obj.metadata.root.selected is True
    # Set a property to not selected
    breadcrumb = ("properties", "col_a")
    set_catalog_stream_selected(
        catalog, stream_name, selected=False, breadcrumb=breadcrumb
    )
    assert catalog_entry_obj.metadata[breadcrumb].selected is False
    # Set a property to selected
    set_catalog_stream_selected(
        catalog, stream_name, selected=True, breadcrumb=breadcrumb
    )
    assert catalog_entry_obj.metadata[breadcrumb].selected is True

    with pytest.raises(
        ValueError,
        match="Catalog entry missing for 'non-existent-stream'",
    ):
        set_catalog_stream_selected(
            catalog,
            "non-existent-stream",
            selected=True,
            breadcrumb=breadcrumb,
        )
