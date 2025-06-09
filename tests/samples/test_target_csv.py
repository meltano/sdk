"""Test tap-to-target sync."""

from __future__ import annotations

import datetime
import importlib.resources
import io
import json
import shutil
import typing as t
import uuid
from contextlib import redirect_stdout
from pathlib import Path

import pytest
import time_machine
from click.testing import CliRunner

from samples.sample_mapper.mapper import StreamTransform
from samples.sample_tap_countries.countries_tap import SampleTapCountries
from samples.sample_tap_fake_people.tap import SampleTapFakePeople
from samples.sample_target_csv.csv_target import SampleTargetCSV
from singer_sdk.testing import (
    get_target_test_class,
    tap_sync_test,
    tap_to_target_sync_test,
    target_sync_test,
)
from tests.conftest import TargetMock

if t.TYPE_CHECKING:
    from pytest_snapshot.plugin import Snapshot

TEST_OUTPUT_DIR = Path(f".output/test_{uuid.uuid4()}/")
SAMPLE_CONFIG = {"target_folder": f"{TEST_OUTPUT_DIR}/"}
DATETIME = datetime.datetime(2022, 1, 1, tzinfo=datetime.timezone.utc)


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


@time_machine.travel(DATETIME, tick=False)
@pytest.mark.snapshot
def test_countries_to_csv(
    csv_config: dict,
    snapshot: Snapshot,
    caplog: pytest.LogCaptureFixture,
):
    tap = SampleTapCountries(config=SAMPLE_TAP_CONFIG, state=None)
    target = SampleTargetCSV(config=csv_config)
    target.max_parallelism = 1

    caplog.set_level("ERROR", "singer_sdk.metrics")

    with caplog.at_level("INFO"):
        tap_stdout, _, target_stdout, _ = tap_to_target_sync_test(tap, target)

    snapshot.assert_match(tap_stdout.read(), "tap.jsonl")
    snapshot.assert_match(target_stdout.read(), "target.jsonl")
    snapshot.assert_match(caplog.text, "singer.log")


@time_machine.travel(DATETIME, tick=False)
@pytest.mark.snapshot
@pytest.mark.parametrize(
    "emit_activate_version_messages",
    [True, False],
    ids=["activate_version", "no_activate_version"],
)
def test_countries_to_csv_mapped(
    snapshot: Snapshot,
    csv_config: dict,
    caplog: pytest.LogCaptureFixture,
    emit_activate_version_messages: bool,
):
    tap = SampleTapCountries(
        config={
            **SAMPLE_TAP_CONFIG,
            "emit_activate_version_messages": emit_activate_version_messages,
        },
        state=None,
    )
    target = SampleTargetCSV(config=csv_config)
    target.max_parallelism = 1
    mapper = StreamTransform(config=COUNTRIES_STREAM_MAPS_CONFIG)

    caplog.set_level("ERROR", "singer_sdk.metrics")

    tap_io = io.TextIOWrapper(io.BytesIO(), encoding="utf-8")
    with redirect_stdout(tap_io), caplog.at_level("INFO"):
        tap.sync_all()

    tap_io.seek(0)
    tap_output = tap_io.read()
    snapshot.assert_match(tap_output, "tap.jsonl")

    tap_io.seek(0)
    mapper_io = io.TextIOWrapper(io.BytesIO(), encoding="utf-8")
    with redirect_stdout(mapper_io), caplog.at_level("INFO"):
        mapper.listen(tap_io)

    mapper_io.seek(0)
    mapper_output = mapper_io.read()
    snapshot.assert_match(mapper_output, "mapper.jsonl")

    mapper_io.seek(0)
    target_io = io.TextIOWrapper(io.BytesIO(), encoding="utf-8")
    with redirect_stdout(target_io), caplog.at_level("INFO"):
        target.listen(mapper_io)

    target_io.seek(0)
    target_output = target_io.read()
    snapshot.assert_match(target_output, "target.jsonl")
    snapshot.assert_match(caplog.text, "singer.log")


@time_machine.travel(DATETIME, tick=False)
@pytest.mark.snapshot
def test_fake_people_to_csv(
    csv_config: dict,
    snapshot: Snapshot,
    caplog: pytest.LogCaptureFixture,
):
    tap = SampleTapFakePeople()
    target = SampleTargetCSV(config=csv_config)
    target.max_parallelism = 1

    caplog.set_level("ERROR", "singer_sdk.metrics")

    with caplog.at_level("INFO"):
        tap_stdout, _, target_stdout, _ = tap_to_target_sync_test(tap, target)

    snapshot.assert_match(tap_stdout.read(), "tap.jsonl")
    snapshot.assert_match(target_stdout.read(), "target.jsonl")
    snapshot.assert_match(caplog.text, "singer.log")


def test_target_batching():
    """Test batch writing behaviors.

    Batch-based target should only drain records after specific threshold
    of time elapsed. Currently this is set to 30 minutes, so we test after 31
    minutes elapsed between checkpoints.
    """
    tap = SampleTapCountries(config=SAMPLE_TAP_CONFIG, state=None)

    buf, _ = tap_sync_test(tap)

    mocked_starttime = datetime.datetime(
        2012,
        1,
        1,
        12,
        0,
        tzinfo=datetime.timezone.utc,
    )
    mocked_jumptotime2 = datetime.datetime(
        2012,
        1,
        1,
        12,
        31,
        tzinfo=datetime.timezone.utc,
    )
    mocked_jumptotime3 = datetime.datetime(
        2012,
        1,
        1,
        13,
        2,
        tzinfo=datetime.timezone.utc,
    )
    countries_record_count = 257

    with time_machine.travel(mocked_starttime, tick=False):
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

    with time_machine.travel(mocked_jumptotime2, tick=False):
        buf.seek(0)
        target_sync_test(target, buf, finalize=False)

        # The first next record should force a batch drain
        assert target.num_records_processed == countries_record_count * 2
        assert len(target.records_written) == countries_record_count + 1
        assert len(target.state_messages_written) == 1

    with time_machine.travel(mocked_jumptotime3, tick=False):
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


SAMPLE_FILENAME = (
    importlib.resources.files("tests.samples") / "resources/messages.jsonl"
)
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
