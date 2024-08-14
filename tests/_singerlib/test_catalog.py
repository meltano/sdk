from __future__ import annotations

import pytest

from singer_sdk._singerlib import (
    Catalog,
    CatalogEntry,
    Metadata,
    MetadataMapping,
    SelectionMask,
    StreamMetadata,
)

METADATA_ARRAY = [
    {
        "breadcrumb": [],
        "metadata": {
            "selected": True,
            "forced-replication-method": "FULL_TABLE",
        },
    },
    {
        "breadcrumb": ["properties", "id"],
        "metadata": {
            "inclusion": "automatic",
            "selected": True,
        },
    },
    {
        "breadcrumb": ["properties", "updated_at"],
        "metadata": {
            "inclusion": "automatic",
            "selected": False,
        },
    },
    {
        "breadcrumb": ["properties", "name"],
        "metadata": {
            "inclusion": "available",
            "selected": True,
        },
    },
    {
        "breadcrumb": ["properties", "an_object"],
        "metadata": {"selected": False},
    },
    {
        "breadcrumb": ["properties", "an_object", "properties", "nested"],
        "metadata": {
            "selected": True,
        },
    },
    {
        "breadcrumb": ["properties", "not_supported_selected"],
        "metadata": {
            "inclusion": "unsupported",
            "selected": True,
        },
    },
    {
        "breadcrumb": ["properties", "not_supported_not_selected"],
        "metadata": {
            "inclusion": "unsupported",
            "selected": False,
        },
    },
    {
        "breadcrumb": ["properties", "selected_by_default"],
        "metadata": {
            "inclusion": "available",
            "selected-by-default": True,
        },
    },
]


def test_selection_mask():
    mask = SelectionMask(
        [
            (("properties", "id"), False),
            (("properties", "an_object"), False),
            (("properties", "an_object", "properties", "a_string"), True),
        ],
    )
    # Missing root breadcrumb is selected
    assert mask[()] is True

    # Explicitly deselected
    assert mask["properties", "id"] is False

    # Missing defaults to parent selection
    assert mask["properties", "name"] is True

    # Explicitly selected
    assert mask["properties", "an_object"] is False

    # Missing defaults to parent selection
    assert mask["properties", "an_object", "properties", "id"] is False

    # Explicitly selected nested property
    assert mask["properties", "an_object", "properties", "a_string"] is True


def test_metadata_mapping():
    mapping = MetadataMapping.from_iterable(METADATA_ARRAY)

    assert (
        mapping[()]
        == mapping.root
        == StreamMetadata(
            selected=True,
            forced_replication_method="FULL_TABLE",
        )
    )
    assert mapping["properties", "id"] == Metadata(
        inclusion=Metadata.InclusionType.AUTOMATIC,
        selected=True,
    )
    assert mapping["properties", "name"] == Metadata(
        inclusion=Metadata.InclusionType.AVAILABLE,
        selected=True,
    )
    assert mapping["properties", "missing"] == Metadata()

    selection_mask = mapping.resolve_selection()
    assert selection_mask[()] is True
    assert selection_mask["properties", "id"] is True
    assert selection_mask["properties", "updated_at"] is True
    assert selection_mask["properties", "name"] is True
    assert selection_mask["properties", "missing"] is True
    assert selection_mask["properties", "an_object"] is False
    assert selection_mask["properties", "an_object", "properties", "nested"] is False
    assert selection_mask["properties", "not_supported_selected"] is False
    assert selection_mask["properties", "not_supported_not_selected"] is False
    assert selection_mask["properties", "selected_by_default"] is True


def test_empty_metadata_mapping():
    """Check that empty metadata mapping results in stream being selected."""
    mapping = MetadataMapping()
    assert mapping._breadcrumb_is_selected(()) is True


def test_catalog_parsing():
    """Validate parsing works for a catalog and its stream entries."""
    catalog_dict = {
        "streams": [
            {
                "tap_stream_id": "test",
                "database_name": "app_db",
                "row_count": 10000,
                "stream_alias": "test_alias",
                "metadata": [
                    {
                        "breadcrumb": [],
                        "metadata": {
                            "inclusion": "available",
                        },
                    },
                    {
                        "breadcrumb": ["properties", "a"],
                        "metadata": {
                            "inclusion": "unsupported",
                        },
                    },
                ],
                "schema": {
                    "type": "object",
                },
            },
        ],
    }
    catalog = Catalog.from_dict(catalog_dict)

    assert catalog.streams[0].tap_stream_id == "test"
    assert catalog.streams[0].database == "app_db"
    assert catalog.streams[0].row_count == 10000
    assert catalog.streams[0].stream_alias == "test_alias"
    assert catalog.get_stream("test").tap_stream_id == "test"
    assert catalog["test"].metadata.to_list() == catalog_dict["streams"][0]["metadata"]
    assert catalog["test"].tap_stream_id == catalog_dict["streams"][0]["tap_stream_id"]
    assert catalog["test"].schema.to_dict() == {"type": "object"}
    assert catalog.to_dict() == catalog_dict

    new = {
        "tap_stream_id": "new",
        "metadata": [],
        "schema": {},
    }
    entry = CatalogEntry.from_dict(new)
    catalog.add_stream(entry)
    assert catalog.get_stream("new") == entry


@pytest.mark.parametrize(
    "schema,key_properties,replication_method,valid_replication_keys,schema_name",
    [
        (
            {"properties": {"id": {"type": "integer"}}, "type": "object"},
            ["id"],
            "FULL_TABLE",
            None,
            None,
        ),
        (
            {
                "properties": {
                    "first_name": {"type": "string"},
                    "last_name": {"type": "string"},
                    "updated_at": {"type": "string", "format": "date-time"},
                },
                "type": "object",
            },
            ["first_name", "last_name"],
            "INCREMENTAL",
            ["updated_at"],
            "users",
        ),
        (
            {
                "properties": {
                    "first_name": {"type": "string"},
                    "last_name": {"type": "string"},
                    "group": {"type": "string"},
                },
                "type": "object",
            },
            ["first_name", "last_name"],
            "FULL_TABLE",
            None,
            None,
        ),
        (
            {},
            [],
            None,
            None,
            None,
        ),
    ],
)
def test_standard_metadata(
    schema: dict,
    key_properties: list[str],
    replication_method: str | None,
    valid_replication_keys: list[str] | None,
    schema_name: str | None,
):
    """Validate generated metadata."""
    metadata = MetadataMapping.get_standard_metadata(
        schema=schema,
        schema_name=schema_name,
        key_properties=key_properties,
        replication_method=replication_method,
        valid_replication_keys=valid_replication_keys,
    )

    stream_metadata = metadata[()]
    assert isinstance(stream_metadata, StreamMetadata)
    assert stream_metadata.table_key_properties == key_properties
    assert stream_metadata.forced_replication_method == replication_method
    assert stream_metadata.valid_replication_keys == valid_replication_keys
    assert stream_metadata.selected is None
    assert stream_metadata.schema_name == schema_name

    for pk in key_properties:
        pk_metadata = metadata["properties", pk]
        assert pk_metadata.inclusion == Metadata.InclusionType.AUTOMATIC
        assert pk_metadata.selected is None

    for rk in valid_replication_keys or []:
        rk_metadata = metadata["properties", rk]
        assert rk_metadata.inclusion == Metadata.InclusionType.AUTOMATIC
        assert rk_metadata.selected is None
