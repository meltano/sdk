"""Test sample sync."""

from __future__ import annotations

import copy
import io
import json
import typing as t
from contextlib import redirect_stdout

import pytest
from click.testing import CliRunner

from samples.sample_tap_countries.countries_tap import SampleTapCountries
from singer_sdk.helpers._catalog import (
    get_selected_schema,
    pop_deselected_record_properties,
)
from singer_sdk.testing import get_tap_test_class
from singer_sdk.testing.config import SuiteConfig

if t.TYPE_CHECKING:
    from pathlib import Path

    from pytest_snapshot.plugin import Snapshot

SAMPLE_CONFIG = {}
SAMPLE_CONFIG_BAD = {"not": "correct"}

# standard tap tests
TestSampleTapCountries = get_tap_test_class(
    tap_class=SampleTapCountries,
    config=SAMPLE_CONFIG,
    suite_config=SuiteConfig(max_records_limit=5),
)


def test_countries_primary_key():
    tap = SampleTapCountries(config=None)
    countries_entry = tap.streams["countries"]._singer_catalog_entry
    metadata_root = countries_entry.metadata.root
    key_props_1 = metadata_root.table_key_properties
    key_props_2 = countries_entry.key_properties
    assert key_props_1 == ("code",), (
        f"Incorrect 'table-key-properties' in catalog: ({key_props_1})\n\n"
        f"Root metadata was: {metadata_root}\n\n"
        f"Catalog entry was: {countries_entry}"
    )
    assert key_props_2 == ("code",), (
        f"Incorrect 'key_properties' in catalog: ({key_props_2})\n\n"
        f"Catalog entry was: {countries_entry}"
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
    )
    assert copied_record == record

    new_schema = get_selected_schema(
        stream_name=stream.name,
        schema=stream.schema,
        mask=stream.metadata.resolve_selection(),
    )
    assert new_schema == stream.schema


def test_batch_mode(outdir):
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
            },
        },
    )

    buf = io.TextIOWrapper(io.BytesIO(), encoding="utf-8")
    with redirect_stdout(buf):
        tap.sync_all()

    buf.seek(0)
    lines = buf.read().splitlines()
    messages = [json.loads(line) for line in lines]

    def tally_messages(messages: list) -> t.Counter:
        """Tally messages."""
        return t.Counter(
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

    assert counter["STATE",] == 2


@pytest.mark.snapshot
def test_write_schema(
    snapshot: Snapshot,
    snapshot_dir: Path,
):
    snapshot.snapshot_dir = snapshot_dir.joinpath("countries_write_schemas")

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(SampleTapCountries.cli, ["--test", "schema"])

    snapshot_name = "countries_write_schemas"
    snapshot.assert_match(result.stdout, snapshot_name)
