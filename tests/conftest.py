"""Top level test fixtures."""

from __future__ import annotations

import pathlib
import platform
import shutil
import typing as t

import pytest

from singer_sdk import typing as th
from singer_sdk.sinks import BatchSink
from singer_sdk.target_base import Target

if t.TYPE_CHECKING:
    from _pytest.config import Config

SYSTEMS = {"linux", "darwin", "windows"}

pytest_plugins = ("singer_sdk.testing.pytest_plugin",)


def pytest_collection_modifyitems(config: Config, items: list[pytest.Item]):
    rootdir = pathlib.Path(config.rootdir)

    for item in items:
        rel_path = pathlib.Path(item.fspath).relative_to(rootdir)

        # Mark all tests under tests/external*/ as 'external'
        if rel_path.parts[1].startswith("external"):
            item.add_marker("external")


def pytest_runtest_setup(item):
    supported_systems = SYSTEMS.intersection(mark.name for mark in item.iter_markers())
    system = platform.system().lower()
    if supported_systems and system not in supported_systems:
        pytest.skip(f"cannot run on platform {system}")


@pytest.fixture(scope="class")
def outdir() -> t.Generator[str, None, None]:
    """Create a temporary directory for cookiecutters and target output."""
    name = ".output/"
    try:
        pathlib.Path(name).mkdir(parents=True)
    except FileExistsError:
        # Directory already exists
        shutil.rmtree(name)
        pathlib.Path(name).mkdir(parents=True)

    yield name
    shutil.rmtree(name)


@pytest.fixture(scope="session")
def snapshot_dir() -> pathlib.Path:
    """Return the path to the snapshot directory."""
    return pathlib.Path("tests/snapshots/")


class BatchSinkMock(BatchSink):
    """A mock Sink class."""

    name = "batch-sink-mock"

    def __init__(
        self,
        target: TargetMock,
        stream_name: str,
        schema: dict,
        key_properties: list[str] | None,
    ):
        """Create the Mock batch-based sink."""
        super().__init__(target, stream_name, schema, key_properties)
        self.target = target

    def process_record(self, record: dict, context: dict) -> None:
        """Tracks the count of processed records."""
        self.target.num_records_processed += 1
        super().process_record(record, context)

    def process_batch(self, context: dict) -> None:
        """Write to mock trackers."""
        self.target.records_written.extend(context["records"])
        self.target.num_batches_processed += 1


class TargetMock(Target):
    """A mock Target class."""

    name = "target-mock"
    config_jsonschema = th.PropertiesList().to_dict()
    default_sink_class = BatchSinkMock

    def __init__(self, *args, **kwargs):
        """Create the Mock target sync."""
        super().__init__(*args, **kwargs)
        self.state_messages_written: list[dict] = []
        self.records_written: list[dict] = []
        self.num_records_processed: int = 0
        self.num_batches_processed: int = 0

    def _write_state_message(self, state: dict):
        """Emit the stream's latest state."""
        super()._write_state_message(state)
        self.state_messages_written.append(state)
