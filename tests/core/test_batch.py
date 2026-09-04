from __future__ import annotations

import re
from dataclasses import asdict

import pytest

from singer_sdk.batch import Batcher
from singer_sdk.helpers._batch import BaseBatchFileEncoding, StorageTarget


def test_batch_get_unsupported_batcher():
    with pytest.raises(ValueError, match="Unsupported batcher"):
        Batcher.get_batcher("unsupported")


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
def test_batch_file_encoding_as_dict(
    encoding: BaseBatchFileEncoding, expected: dict
) -> None:
    """Test encoding as dict."""
    assert asdict(encoding) == expected


def test_batch_storage_target_defaults() -> None:
    target = StorageTarget()
    assert target.root.startswith("file://")
    assert target.root.endswith("singer-sdk")
    assert target.prefix is None
    assert target.params == {}

    target = StorageTarget(root="file://path/to/files")
    assert target.root == "file://path/to/files"
    assert target.prefix is None
    assert target.params == {}


def test_batch_storage_target_round_trip() -> None:
    target = StorageTarget(root="file://path/to/files")
    d = target.asdict()
    assert d == {"root": "file://path/to/files", "prefix": None, "params": {}}
    assert StorageTarget.from_dict(d) == target


@pytest.mark.parametrize(
    "file_url,storage_target",
    [
        pytest.param(
            "file:///Users/sdk/path/to/file",
            StorageTarget("file:///Users/sdk/path/to", params={}),
            marks=(pytest.mark.linux, pytest.mark.darwin),
            id="local",
        ),
        pytest.param(
            "file:///Users/sdk/path/to/file",
            StorageTarget("file:///D:/Users/sdk/path/to", params={}),
            marks=(pytest.mark.windows,),
            id="windows-local",
        ),
        pytest.param(
            "s3://test_bucket/object_prefix/prefix--file.jsonl.gz",
            StorageTarget("s3://test_bucket/object_prefix", params={}),
            id="s3",
        ),
    ],
)
def test_batch_storage_target_from_url(file_url: str, storage_target: StorageTarget):
    """Test storage target from URL."""
    head, _ = StorageTarget.split_url(file_url)
    assert StorageTarget.from_url(head) == storage_target


def test_batch_storage_target_get_url() -> None:
    target = StorageTarget(root="file://path/to/files")
    url = target.get_url("filename")
    assert url.startswith("file://")
    assert re.match(r"file:\/\/\/.+\/path\/to\/files\/filename", url)


def test_batch_storage_target_get_s3_url() -> None:
    storage = StorageTarget("s3://test_bucket")
    url = storage.get_url("prefix--file.jsonl.gz")
    assert url.startswith("s3://test_bucket/prefix--file.jsonl.gz")


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
def test_batch_storage_target_split_url(file_url: str, expected: tuple):
    """Test storage target split URL."""
    assert StorageTarget.split_url(file_url) == expected
