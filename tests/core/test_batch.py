from dataclasses import asdict
from urllib.parse import urlparse

import pytest

from singer_sdk.helpers._batch import (
    BaseBatchFileEncoding,
    JSONLinesEncoding,
    ParquetEncoding,
    StorageTarget,
)


@pytest.mark.parametrize(
    "encoding,expected",
    [
        (JSONLinesEncoding("gzip"), {"compression": "gzip", "format": "jsonl"}),
        (JSONLinesEncoding(), {"compression": None, "format": "jsonl"}),
        (ParquetEncoding("gzip"), {"compression": "gzip", "format": "parquet"}),
        (ParquetEncoding(), {"compression": None, "format": "parquet"}),
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

    with storage.fs(create=True) as fs:
        url = fs.geturl(prefix)
        assert url.startswith(file_scheme)
        assert url.replace("\\", "/").endswith(expected)


@pytest.mark.parametrize(
    "file_url,root",
    [
        pytest.param(
            "file:///Users/sdk/path/to/file",
            "file:///Users/sdk/path/to",
            id="local",
        ),
    ],
)
def test_storage_from_url(file_url: str, root: str):
    """Test storage target from URL."""
    head, _ = StorageTarget.split_url(file_url)
    target = StorageTarget.from_url(head)
    assert target.root == root


@pytest.mark.parametrize(
    "file_url,expected",
    [
        pytest.param(
            "file:///Users/sdk/path/to/file",
            ("file:///Users/sdk/path/to", "file"),
            id="local",
        ),
        pytest.param(
            "s3://bucket/path/to/file",
            ("s3://bucket/path/to", "file"),
            id="s3",
        ),
        pytest.param(
            "file://C:\\Users\\sdk\\path\\to\\file",
            ("file://C:\\Users\\sdk\\path\\to", "file"),
            marks=(pytest.mark.windows,),
            id="windows-local",
        ),
        pytest.param(
            "file://\\\\remotemachine\\C$\\batches\\file",
            ("file://\\\\remotemachine\\C$\\batches", "file"),
            marks=(pytest.mark.windows,),
            id="windows-local",
        ),
    ],
)
def test_storage_split_url(file_url: str, expected: tuple):
    """Test storage target split URL."""
    assert StorageTarget.split_url(file_url) == expected
