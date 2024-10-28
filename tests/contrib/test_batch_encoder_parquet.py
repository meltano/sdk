"""Tests for the Parquet batch encoder."""

from __future__ import annotations

import typing as t

from singer_sdk.contrib.batch_encoder_parquet import ParquetBatcher
from singer_sdk.helpers._batch import BatchConfig, ParquetEncoding, StorageTarget

if t.TYPE_CHECKING:
    from pathlib import Path


def test_batcher(tmp_path: Path) -> None:
    root = tmp_path.joinpath("batches")
    root.mkdir()
    config = BatchConfig(
        encoding=ParquetEncoding(),
        storage=StorageTarget(root=str(root)),
        batch_size=2,
    )
    batcher = ParquetBatcher("tap", "stream", config)
    records = [
        {"id": 1, "numeric": "1.0"},
        {"id": 2, "numeric": "2.0"},
        {"id": 3, "numeric": "3.0"},
    ]
    batches = list(batcher.get_batches(records))
    assert len(batches) == 2
    assert batches[0][0].endswith(".parquet")


def test_batcher_gzip(tmp_path: Path) -> None:
    root = tmp_path.joinpath("batches")
    root.mkdir()
    config = BatchConfig(
        encoding=ParquetEncoding(compression="gzip"),
        storage=StorageTarget(root=str(root)),
        batch_size=2,
    )
    batcher = ParquetBatcher("tap", "stream", config)
    records = [
        {"id": 1, "numeric": "1.0"},
        {"id": 2, "numeric": "2.0"},
        {"id": 3, "numeric": "3.0"},
    ]
    batches = list(batcher.get_batches(records))
    assert len(batches) == 2
    assert batches[0][0].endswith(".parquet.gz")
