from singer_sdk.helpers._singer import Catalog, CatalogEntry, MetadataMapping


def test_1():
    metadata_array = [
        {
            "breadcrumb": [],
            "metadata": {
                "inclusion": "available",
            },
        },
        {
            "breadcrumb": ["a"],
            "metadata": {
                "inclusion": "unsupported",
            },
        },
    ]
    mapping = MetadataMapping.from_iterable(metadata_array)
    assert mapping[()] == {"inclusion": "available"}
    assert mapping[("a",)] == {"inclusion": "unsupported"}

    assert mapping.to_list() == metadata_array


def test_2():
    entry_dict = {
        "tap_stream_id": "test",
        "metadata": [
            {
                "breadcrumb": [],
                "metadata": {
                    "inclusion": "available",
                },
            },
            {
                "breadcrumb": ["a"],
                "metadata": {
                    "inclusion": "unsupported",
                },
            },
        ],
        "schema": {
            "type": "object",
        },
    }
    entry = CatalogEntry.from_dict(entry_dict)

    assert entry.metadata.to_list() == entry_dict["metadata"]
    assert entry.tap_stream_id == entry_dict["tap_stream_id"]
    assert entry.schema.to_dict() == {"type": "object"}
    assert entry.to_dict() == entry_dict


def test_catalog_parsing():
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
