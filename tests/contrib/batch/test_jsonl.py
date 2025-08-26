from __future__ import annotations

import decimal
import re
from dataclasses import asdict

import pytest

from singer_sdk.batch import Batcher
from singer_sdk.contrib.batch_encoder_jsonl import JSONLinesBatcher
from singer_sdk.helpers._batch import (
    BaseBatchFileEncoding,
    BatchConfig,
    StorageTarget,
)


@pytest.mark.parametrize(
    "encoding,expected",
    [
        (
            BaseBatchFileEncoding(format="jsonl", compression="gzip"),
            {"compression": "gzip", "format": "jsonl"},
        ),
        (
            BaseBatchFileEncoding(format="jsonl"),
            {"compression": None, "format": "jsonl"},
        ),
        (
            BaseBatchFileEncoding(format="parquet", compression="gzip"),
            {"compression": "gzip", "format": "parquet"},
        ),
        (
            BaseBatchFileEncoding(format="parquet"),
            {"compression": None, "format": "parquet"},
        ),
    ],
    ids=[
        "jsonl-compression-gzip",
        "jsonl-compression-none",
        "parquet-compression-gzip",
        "parquet-compression-none",
    ],
)
def test_encoding_as_dict(encoding: BaseBatchFileEncoding, expected: dict) -> None:
    """Test encoding as dict."""
    assert asdict(encoding) == expected


@pytest.mark.parametrize(
    "file_scheme,root,prefix,expected",
    [
        (
            "file://",
            "root_dir",
            "prefix--file.jsonl.gz",
            "root_dir/prefix--file.jsonl.gz",
        ),
        (
            "file://",
            "root_dir",
            "prefix--file.parquet.gz",
            "root_dir/prefix--file.parquet.gz",
        ),
    ],
    ids=["jsonl-url", "parquet-url"],
)
def test_storage_get_url(file_scheme, root, prefix, expected):
    storage = StorageTarget(file_scheme + root)

    url = storage.get_url(prefix)
    assert url.startswith(file_scheme)
    assert url.replace("\\", "/").endswith(expected)


def test_storage_get_s3_url():
    storage = StorageTarget("s3://test_bucket")
    url = storage.get_url("prefix--file.jsonl.gz")
    assert url.startswith("s3://test_bucket/prefix--file.jsonl.gz")


@pytest.mark.parametrize(
    "file_url,root",
    [
        pytest.param(
            "file:///Users/sdk/path/to/file",
            "file:///Users/sdk/path/to",
            marks=(pytest.mark.linux, pytest.mark.darwin),
            id="local",
        ),
        pytest.param(
            "file:///Users/sdk/path/to/file",
            "file:///D:/Users/sdk/path/to",
            marks=(pytest.mark.windows,),
            id="windows-local",
        ),
        pytest.param(
            "s3://test_bucket/object_prefix/prefix--file.jsonl.gz",
            "s3://test_bucket/object_prefix",
            id="s3",
        ),
    ],
)
def test_storage_from_url(file_url: str, root: str):
    """Test storage target from URL."""
    head, _ = StorageTarget.split_url(file_url)
    target = StorageTarget.from_url(head)
    assert target.root == root


def test_get_unsupported_batcher():
    with pytest.raises(ValueError, match="Unsupported batcher"):
        Batcher.get_batcher("unsupported")


@pytest.mark.parametrize(
    "file_url,expected",
    [
        pytest.param(
            "file:///Users/sdk/path/to/file",
            ("file:///Users/sdk/path/to", "file"),
            marks=(pytest.mark.linux, pytest.mark.darwin),
            id="local",
        ),
        pytest.param(
            "s3://bucket/path/to/file",
            ("s3://bucket/path/to", "file"),
            id="s3",
        ),
        pytest.param(
            "file://C:\\Users\\sdk\\path\\to\\file",
            ("file:///C:/Users/sdk/path/to", "file"),
            marks=(pytest.mark.windows,),
            id="windows-local",
        ),
        pytest.param(
            "file://\\\\remotemachine\\C$\\batches\\file",
            ("file://///remotemachine/C$/batches", "file"),
            marks=(pytest.mark.windows,),
            id="windows-remote",
        ),
    ],
)
def test_storage_split_url(file_url: str, expected: tuple):
    """Test storage target split URL."""
    assert StorageTarget.split_url(file_url) == expected


def test_json_lines_batcher():
    batcher = JSONLinesBatcher(
        "tap-test",
        "stream-test",
        batch_config=BatchConfig(
            encoding=BaseBatchFileEncoding(format="jsonl", compression="gzip"),
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
        re.match(r".*tap-test--stream-test-.*\.json.gz", filepath)
        for batch in batches
        for filepath in batch
    )


def test_batcher_with_jsonl_encoding():
    batcher = Batcher(
        "tap-test",
        "stream-test",
        batch_config=BatchConfig(
            encoding=BaseBatchFileEncoding(format="jsonl", compression="gzip"),
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
        re.match(r".*tap-test--stream-test-.*\.json.gz", filepath)
        for batch in batches
        for filepath in batch
    )
