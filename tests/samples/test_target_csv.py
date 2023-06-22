"""Test tap-to-target sync."""
from __future__ import annotations

import json
import shutil
import typing as t
import uuid
from pathlib import Path

import pytest
from click.testing import CliRunner
from freezegun import freeze_time

from samples.sample_mapper.mapper import StreamTransform
from samples.sample_tap_countries.countries_tap import SampleTapCountries
from samples.sample_target_csv.csv_target import SampleTargetCSV
from singer_sdk.testing import (
    get_target_test_class,
    sync_end_to_end,
    tap_sync_test,
    tap_to_target_sync_test,
    target_sync_test,
)
from tests.conftest import TargetMock

TEST_OUTPUT_DIR = Path(f".output/test_{uuid.uuid4()}/")
SAMPLE_CONFIG = {"target_folder": f"{TEST_OUTPUT_DIR}/"}


StandardTests = get_target_test_class(
    target_class=SampleTargetCSV,
    config=SAMPLE_CONFIG,
)


class TestSampleTargetCSV(StandardTests):
    """Standard Target Tests."""

    @pytest.fixture(scope="class")
    def test_output_dir(self):
        return TEST_OUTPUT_DIR

    @pytest.fixture(scope="class")
    def resource(self, test_output_dir):
        test_output_dir.mkdir(parents=True, exist_ok=True)
        yield test_output_dir
        shutil.rmtree(test_output_dir)


SAMPLE_TAP_CONFIG: dict[str, t.Any] = {}
COUNTRIES_STREAM_MAPS_CONFIG: dict[str, t.Any] = {
    "stream_maps": {"continents": {}, "__else__": None},
}


def test_countries_to_csv(csv_config: dict):
    tap = SampleTapCountries(config=SAMPLE_TAP_CONFIG, state=None)
    target = SampleTargetCSV(config=csv_config)
    tap_to_target_sync_test(tap, target)


def test_countries_to_csv_mapped(csv_config: dict):
    tap = SampleTapCountries(config=SAMPLE_TAP_CONFIG, state=None)
    target = SampleTargetCSV(config=csv_config)
    mapper = StreamTransform(config=COUNTRIES_STREAM_MAPS_CONFIG)
    sync_end_to_end(tap, target, mapper)


def test_target_batching():
    """Test batch writing behaviors.

    Batch-based target should only drain records after specific threshold
    of time elapsed. Currently this is set to 30 minutes, so we test after 31
    minutes elapsed between checkpoints.
    """
    tap = SampleTapCountries(config=SAMPLE_TAP_CONFIG, state=None)

    buf, _ = tap_sync_test(tap)

    mocked_starttime = "2012-01-01 12:00:00"
    mocked_jumptotime2 = "2012-01-01 12:31:00"
    mocked_jumptotime3 = "2012-01-01 13:02:00"
    countries_record_count = 257

    with freeze_time(mocked_starttime):
        target = TargetMock(config={})
        target.max_parallelism = 1  # Limit unit test to 1 process
        assert target.num_records_processed == 0
        assert len(target.records_written) == 0
        assert len(target.state_messages_written) == 0

        # `buf` now contains the output of the full tap sync
        target_sync_test(target, buf, finalize=False)

        assert target.num_records_processed == countries_record_count
        assert len(target.records_written) == 0  # Drain not yet called
        assert len(target.state_messages_written) == 0  # Drain not yet called

    with freeze_time(mocked_jumptotime2):
        buf.seek(0)
        target_sync_test(target, buf, finalize=False)

        # The first next record should force a batch drain
        assert target.num_records_processed == countries_record_count * 2
        assert len(target.records_written) == countries_record_count + 1
        assert len(target.state_messages_written) == 1

    with freeze_time(mocked_jumptotime3):
        buf.seek(0)
        target_sync_test(target, buf, finalize=False)

        # The first next record should force a batch drain
        assert target.num_records_processed == countries_record_count * 3
        assert len(target.records_written) == (countries_record_count * 2) + 1
        assert len(target.state_messages_written) == 2

        # Should force a final STATE message
        target_sync_test(target, input=None, finalize=True)

    assert target.num_records_processed == countries_record_count * 3
    assert len(target.state_messages_written) == 3
    assert target.state_messages_written[-1] == {
        "bookmarks": {"continents": {}, "countries": {}},
    }


SAMPLE_FILENAME = Path(__file__).parent / Path("./resources/messages.jsonl")
EXPECTED_OUTPUT = """"id"	"name"
1	"Chris"
2	"Mike"
"""


@pytest.fixture
def target(csv_config: dict):
    return SampleTargetCSV(config=csv_config)


@pytest.fixture
def cli_runner():
    return CliRunner()


@pytest.fixture
def config_file_path(target):
    try:
        path = Path(target.config["target_folder"]) / "./config.json"
        with path.open("w") as f:
            f.write(json.dumps(dict(target.config)))
        yield path
    finally:
        path.unlink()


def test_input_arg(cli_runner, config_file_path, target):
    result = cli_runner.invoke(
        target.cli,
        [
            "--config",
            config_file_path,
            "--input",
            SAMPLE_FILENAME,
        ],
    )

    assert result.exit_code == 0

    output = Path(target.config["target_folder"]) / "./users.csv"
    with output.open() as f:
        assert f.read() == EXPECTED_OUTPUT
