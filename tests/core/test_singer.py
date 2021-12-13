from typing import List

import pytest

from singer_sdk.helpers._singer import Catalog, CatalogEntry, Metadata, MetadataMapping


def test_catalog_parsing():
    """Validate parsing works for a catalog and its stream entries."""
    catalog_dict = {
        "streams": [
            {
                "tap_stream_id": "test",
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
    "schema,key_properties,replication_method",
    [
        (
            {"properties": {"id": {"type": "integer"}}, "type": "object"},
            ["id"],
            "FULL_TABLE",
        ),
        (
            {
                "properties": {
                    "first_name": {"type": "string"},
                    "last_name": {"type": "string"},
                },
                "type": "object",
            },
            ["first_name", "last_name"],
            "FULL_TABLE",
        ),
        (
            {},
            [],
            None,
        ),
    ],
)
def test_standard_metadata(
    schema: dict,
    key_properties: List[str],
    replication_method: str,
):
    """Validate generated metadata."""
    metadata = MetadataMapping.get_standard_metadata(
        schema=schema,
        schema_name="test",
        key_properties=key_properties,
        replication_method=replication_method,
    )

    stream_metadata = metadata[()]
    assert stream_metadata.table_key_properties == key_properties
    assert stream_metadata.forced_replication_method == replication_method
    assert stream_metadata.valid_replication_keys is None
    assert stream_metadata.selected is None

    for pk in key_properties:
        pk_metadata = metadata[("properties", pk)]
        assert pk_metadata.inclusion == Metadata.InclusionType.AUTOMATIC
        assert pk_metadata.selected is None
