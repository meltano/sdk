from __future__ import annotations

import decimal
import importlib.util
import re
import typing as t
from unittest import mock

import pytest

from singer_sdk.batch import Batcher
from singer_sdk.contrib.batch_encoder_arrow import ArrowBatcher
from singer_sdk.exceptions import MissingDependencyError
from singer_sdk.helpers._batch import (
    BaseBatchFileEncoding,
    BatchConfig,
    BatchFileFormat,
    StorageTarget,
)
from singer_sdk.sinks import BatchSink
from tests.contrib.batch.helpers import block_import

if t.TYPE_CHECKING:
    from pathlib import Path


def is_pyarrow_installed():
    module_spec = importlib.util.find_spec("pyarrow")
    return module_spec is not None


skip_if_no_pyarrow = pytest.mark.skipif(
    not is_pyarrow_installed(),
    reason="requires pyarrow",
)


@skip_if_no_pyarrow
def test_batcher(tmp_path: Path) -> None:
    root = tmp_path.joinpath("batches")
    root.mkdir()
    config = BatchConfig(
        encoding=BaseBatchFileEncoding(format="arrow"),
        storage=StorageTarget(root=str(root)),
        batch_size=2,
    )
    batcher = ArrowBatcher("tap", "stream", config)
    records = [
        {"id": 1, "numeric": "1.0"},
        {"id": 2, "numeric": "2.0"},
        {"id": 3, "numeric": "3.0"},
    ]
    batches = list(batcher.get_batches(records))
    assert len(batches) == 2
    assert batches[0][0].endswith(".arrow")


@skip_if_no_pyarrow
def test_batcher_with_arrow_encoding():
    batcher = Batcher(
        "tap-test",
        "stream-test",
        batch_config=BatchConfig(
            encoding=BaseBatchFileEncoding(format="arrow"),
            storage=StorageTarget("file:///tmp/sdk-batches"),
            batch_size=2,
        ),
    )
    records = [
        {"id": 1, "numeric": decimal.Decimal("1.0")},
        {"id": 2, "numeric": decimal.Decimal("2.0")},
        {"id": 3, "numeric": decimal.Decimal("3.0")},
    ]

    batches = list(batcher.get_batches(records))
    assert len(batches) == 2
    assert all(len(batch) == 1 for batch in batches)
    assert all(
        re.match(r".*tap-test--stream-test-.*\.arrow", filepath)
        for batch in batches
        for filepath in batch
    )


def test_sink_reads_arrow_batch_files(tmp_path: Path) -> None:
    """Round-trip: ArrowBatcher output is readable by a sink using nanoarrow."""
    pytest.importorskip("pyarrow")
    pytest.importorskip("nanoarrow")

    received: list[dict] = []

    class MySink(BatchSink):
        def process_batch(self, context: dict) -> None:
            received.extend(context["records"])

    root = tmp_path.joinpath("batches")
    root.mkdir()
    config = BatchConfig(
        encoding=BaseBatchFileEncoding(format="arrow"),
        storage=StorageTarget(root=f"file://{root}"),
        batch_size=2,
    )
    records = [
        {"id": 1, "name": "a", "nested": {"x": 1}},
        {"id": 2, "name": None, "nested": {"x": 2}},
        {"id": 3, "name": "c", "nested": {"x": 3}},
    ]
    files = [
        f
        for batch in ArrowBatcher("tap", "s", config).get_batches(records)
        for f in batch
    ]

    sink = MySink(
        target=mock.MagicMock(),
        stream_name="s",
        schema={"type": "object", "properties": {}},
        key_properties=None,
    )
    sink.process_batch_files(BaseBatchFileEncoding(format=BatchFileFormat.ARROW), files)

    assert received == records


def test_batcher_missing_pyarrow(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    block_import(monkeypatch, "pyarrow")
    config = BatchConfig(
        encoding=BaseBatchFileEncoding(format="arrow"),
        storage=StorageTarget(root=str(tmp_path)),
    )
    with pytest.raises(MissingDependencyError, match="pip install 'pyarrow'"):
        next(ArrowBatcher("tap", "stream", config).get_batches([{"id": 1}]))
