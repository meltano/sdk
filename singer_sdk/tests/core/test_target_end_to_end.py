"""Test tap-to-target sync."""

import io
from contextlib import redirect_stdout
from typing import Any, Dict, List, Optional

from freezegun import freeze_time

from singer_sdk import typing as th
from singer_sdk.samples.sample_tap_countries.countries_tap import SampleTapCountries
from singer_sdk.samples.sample_target_csv.csv_target import SampleTargetCSV
from singer_sdk.sinks import BatchSink
from singer_sdk.target_base import Target

SAMPLE_FILENAME = "/tmp/testfile.countries"
SAMPLE_TAP_CONFIG: Dict[str, Any] = {}


class BatchSinkMock(BatchSink):
    """A mock Sink class."""

    name = "batch-sink-mock"

    def __init__(
        self,
        target: "TargetMock",
        stream_name: str,
        schema: Dict,
        key_properties: Optional[List[str]],
    ):
        """Create the Mock batch-based sink."""
        super().__init__(target, stream_name, schema, key_properties)
        self.target = target

    def process_record(self, record: dict, context: dict) -> Optional[dict]:
        """Tracks the count of processed records."""
        self.target.num_records_processed += 1
        return super().process_record(record, context)

    def process_batch(self, context: dict) -> None:
        """Write to mock trackers."""
        self.target.records_written.extend(context["records"])
        self.target.num_batches_processed += 1


class TargetMock(Target):
    """A mock Target class."""

    name = "target-mock"
    config_jsonschema = th.PropertiesList().to_dict()
    default_sink_class = BatchSinkMock

    def __init__(self):
        """Create the Mock target sync."""
        super().__init__(config={})
        self.state_messages_written: List[dict] = []
        self.records_written: List[dict] = []
        self.num_records_processed: int = 0
        self.num_batches_processed: int = 0

    def _write_state_message(self, state: dict):
        """Emit the stream's latest state."""
        super()._write_state_message(state)
        self.state_messages_written.append(state)


def sync_end_to_end(tap, target):
    """Test and end-to-end sink from the tap to the target."""
    buf = io.StringIO()
    with redirect_stdout(buf):
        tap.sync_all()
    buf.seek(0)
    target._process_lines(buf)
    target._process_endofpipe()


def test_countries_to_csv(csv_config: dict):
    tap = SampleTapCountries(config=SAMPLE_TAP_CONFIG, state=None)
    target = SampleTargetCSV(config=csv_config)
    sync_end_to_end(tap, target)


def test_target_batching():
    """Test batch writing behaviors.

    Batch-based target should only drain records after specific threshold
    of time elapsed. Currently this is set to 30 minutes, so we test after 31
    minutes elapsed between checkpoints.
    """
    tap = SampleTapCountries(config=SAMPLE_TAP_CONFIG, state=None)
    buf = io.StringIO()
    with redirect_stdout(buf):
        tap.sync_all()

    mocked_starttime = "2012-01-01 12:00:00"
    mocked_jumptotime2 = "2012-01-01 12:31:00"
    mocked_jumptotime3 = "2012-01-01 13:02:00"
    countries_record_count = 257

    with freeze_time(mocked_starttime):
        target = TargetMock()
        target.max_parallelism = 1  # Limit unit test to 1 process
        assert target.num_records_processed == 0
        assert len(target.records_written) == 0
        assert len(target.state_messages_written) == 0

        # `buf` now contains the output of the full tap sync
        buf.seek(0)
        target._process_lines(buf)

        # sync_end_to_end(tap, target)

        assert target.num_records_processed == countries_record_count
        assert len(target.records_written) == 0  # Drain not yet called
        assert len(target.state_messages_written) == 0  # Drain not yet called

    with freeze_time(mocked_jumptotime2):
        buf.seek(0)
        target._process_lines(buf)

        # The first next record should force a batch drain
        assert target.num_records_processed == countries_record_count * 2
        assert len(target.records_written) == countries_record_count + 1
        assert len(target.state_messages_written) == 1

    with freeze_time(mocked_jumptotime3):
        buf.seek(0)
        target._process_lines(buf)

        # The first next record should force a batch drain
        assert target.num_records_processed == countries_record_count * 3
        assert len(target.records_written) == (countries_record_count * 2) + 1
        assert len(target.state_messages_written) == 2

        target._process_endofpipe()  # Should force a final STATE message

    assert target.num_records_processed == countries_record_count * 3
    assert len(target.state_messages_written) == 3
    assert target.state_messages_written[-1] == {
        "bookmarks": {"continents": {}, "countries": {}}
    }
