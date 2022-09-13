"""Test sample sync."""

import copy
import io
import json
import logging
from contextlib import redirect_stdout
from re import I
from typing import Counter

from samples.sample_tap_countries.countries_tap import SampleTapCountries
from singer_sdk.helpers._catalog import (
    get_selected_schema,
    pop_deselected_record_properties,
)

SAMPLE_CONFIG_BAD = {"not": "correct"}


def test_countries_sync_all():
    """Test sync_all() for countries sample."""
    tap = SampleTapCountries(config=None)
    tap.sync_all()


def test_countries_primary_key():
    tap = SampleTapCountries(config=None)
    countries_entry = tap.streams["countries"]._singer_catalog_entry
    metadata_root = countries_entry.metadata.root
    key_props_1 = metadata_root.table_key_properties
    key_props_2 = countries_entry.key_properties
    assert key_props_1 == ["code"], (
        f"Incorrect 'table-key-properties' in catalog: ({key_props_1})\n\n"
        f"Root metadata was: {metadata_root}\n\n"
        f"Catalog entry was: {countries_entry}"
    )
    assert key_props_2 == ["code"], (
        f"Incorrect 'key_properties' in catalog: ({key_props_2})\n\n"
        "Catalog entry was: {countries_entry}"
    )


def test_with_catalog_mismatch():
    """Test catalog apply with no matching stream catalog entries."""
    tap = SampleTapCountries(config=None, catalog={"streams": []})
    for stream in tap.streams.values():
        # All streams should be deselected:
        assert not stream.selected


def test_with_catalog_entry():
    """Test catalog apply with a matching stream catalog entry for one stream."""
    tap = SampleTapCountries(
        config=None,
        catalog={
            "streams": [
                {
                    "tap_stream_id": "continents",
                    "schema": {
                        "type": "object",
                        "properties": {},
                    },
                    "metadata": [],
                },
            ],
        },
    )
    assert tap.streams["continents"].selected
    assert not tap.streams["countries"].selected

    stream = tap.streams["continents"]
    record = next(stream.get_records(None))
    copied_record = copy.deepcopy(record)

    pop_deselected_record_properties(
        record=copied_record,
        schema=stream.schema,
        mask=stream.mask,
        logger=logging.getLogger(),
    )
    assert copied_record == record

    new_schema = get_selected_schema(
        stream_name=stream.name,
        schema=stream.schema,
        mask=stream.metadata.resolve_selection(),
        logger=logging.getLogger(),
    )
    assert new_schema == stream.schema


def test_batch_mode(monkeypatch, outdir):
    """Test batch mode."""
    tap = SampleTapCountries(
        config={
            "batch_config": {
                "encoding": {
                    "format": "jsonl",
                    "compression": "gzip",
                },
                "storage": {
                    "root": outdir,
                    "prefix": "pytest-countries-",
                },
            }
        }
    )

    buf = io.StringIO()
    with redirect_stdout(buf):
        tap.sync_all()

    buf.seek(0)
    lines = buf.read().splitlines()
    messages = [json.loads(line) for line in lines]

    def tally_messages(messages: list) -> Counter:
        """Tally messages."""
        return Counter(
            (message["type"], message["stream"])
            if message["type"] != "STATE"
            else (message["type"],)
            for message in messages
        )

    counter = tally_messages(messages)
    assert counter["SCHEMA", "continents"] == 1
    assert counter["BATCH", "continents"] == 1

    assert counter["SCHEMA", "countries"] == 1
    assert counter["BATCH", "countries"] == 1

    assert counter[("STATE",)] == 2
