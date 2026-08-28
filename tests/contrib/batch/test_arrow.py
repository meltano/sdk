from __future__ import annotations

import decimal
import importlib.util
import re
import typing as t

import pytest

from singer_sdk.batch import Batcher
from singer_sdk.contrib.batch_encoder_arrow import ArrowBatcher
from singer_sdk.helpers._batch import (
    BaseBatchFileEncoding,
    BatchConfig,
    StorageTarget,
)

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
