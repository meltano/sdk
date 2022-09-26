from dataclasses import asdict
from urllib.parse import urlparse

import pytest

from singer_sdk.helpers._batch import (
    BaseBatchFileEncoding,
    JSONLinesEncoding,
    StorageTarget,
)


@pytest.mark.parametrize(
    "encoding,expected",
    [
        (JSONLinesEncoding("gzip"), {"compression": "gzip", "format": "jsonl"}),
        (JSONLinesEncoding(), {"compression": None, "format": "jsonl"}),
    ],
    ids=["jsonl-compression-gzip", "jsonl-compression-none"],
)
def test_encoding_as_dict(encoding: BaseBatchFileEncoding, expected: dict) -> None:
    """Test encoding as dict."""
    assert asdict(encoding) == expected


def test_storage_get_url():
    storage = StorageTarget("file://root_dir")

    with storage.fs(create=True) as fs:
        url = fs.geturl("prefix--file.jsonl.gz")
        assert url.startswith("file://")
        assert url.replace("\\", "/").endswith("root_dir/prefix--file.jsonl.gz")


def test_storage_from_url():
    url = urlparse("s3://bucket/path/to/file?region=us-east-1")
    target = StorageTarget.from_url(url)
    assert target.root == "s3://bucket"
    assert target.prefix is None
    assert target.params == {"region": ["us-east-1"]}
