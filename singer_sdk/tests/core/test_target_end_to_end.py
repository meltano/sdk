"""Test tap-to-target sync."""

import io
from contextlib import redirect_stdout

from typing import Dict, Any, List, Optional

from singer_sdk import typing as th
from singer_sdk.target_base import Target
from singer_sdk.plugin_base import PluginBase
from singer_sdk.sinks import BatchSink

from singer_sdk.samples.sample_tap_countries.countries_tap import SampleTapCountries
from singer_sdk.samples.sample_target_csv.csv_target import SampleTargetCSV

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
        super().__init__(target, stream_name, schema, key_properties)
        self.target = target
        self.records_written: List[dict] = []
        self.num_batches_processed: int = 0

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
        super().__init__(config={})
        self.state_messages_written: List[dict] = []
        self.records_written: List[dict] = []
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


# TODO: Add pytest.parametrize here
def test_target_batching():
    target = TargetMock()

    # TODO: Use a sample row generator or a Mock Tap instead of TapCountries:
    tap = SampleTapCountries(config=SAMPLE_TAP_CONFIG, state=None)
    buf = io.StringIO()
    with redirect_stdout(buf):
        tap.sync_all()

    # `buf` now contains the output of a full tap sync
    buf.seek(0)
    target._process_lines(buf)

    sync_end_to_end(tap, target)

    # TODO: Set these to real checks (currently arbitrary/sample)
    assert len(target.records_written) == 100
    assert len(target.state_messages_written) == 2
    assert target.state_messages_written[-1] == {"expected": "final state"}

    buf.seek(0)
    target._process_lines(buf[0:1])  # Should force a drain of the sink

    # Should have forced an additional state and record message
    assert len(target.records_written) == 101
    assert len(target.state_messages_written) == 3

    target._process_endofpipe()  # Should force a final STATE message

    assert len(target.state_messages_written) == 4
    assert target.state_messages_written[-1] == {"expected": "final state"}
