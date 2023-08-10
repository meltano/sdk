from __future__ import annotations

import decimal
import re
from dataclasses import asdict

import pytest

from singer_sdk.batch import CSVBatcher, JSONLinesBatcher
from singer_sdk.exceptions import (
    UnsupportedBatchCompressionError,
    UnsupportedBatchFormatError,
)
from singer_sdk.helpers._batch import (
    BaseBatchFileEncoding,
    BatchConfig,
    BatchFileFormat,
    CSVEncoding,
    JSONLinesEncoding,
    StorageTarget,
)
from singer_sdk.streams.core import Stream
from singer_sdk.tap_base import Tap


class DefaultBatchesStream(Stream):
    name = "default_batches"
    schema = {  # noqa: RUF012
        "properties": {
            "id": {"type": "integer"},
            "name": {"type": "string"},
            "amount": {"type": "number"},
        },
    }

    def get_records(self, _):
        yield {"id": 1, "name": "foo", "amount": decimal.Decimal("1.23")}
        yield {"id": 2, "name": "bar", "amount": decimal.Decimal("4.56")}
        yield {"id": 3, "name": "baz", "amount": decimal.Decimal("7.89")}


class CustomBatchesStream(DefaultBatchesStream):
    name = "custom_batches"

    def get_batches(self, batch_config, context=None):
        if batch_config.encoding.format == BatchFileFormat.CSV:
            csv_batcher = CSVBatcher(self.tap_name, self.name, batch_config)
            records = self._sync_records(context, write_messages=False)
            for manifest in csv_batcher.get_batches(records=records):
                yield batch_config.encoding, manifest
        else:
            yield from super().get_batches(batch_config, context)


class BatchedTap(Tap):
    name = "tap-batched"
    config_jsonschema = {"properties": {}}  # noqa: RUF012

    def discover_streams(self):
        return [DefaultBatchesStream(self), CustomBatchesStream(self)]


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


def test_storage_get_s3_url():
    storage = StorageTarget("s3://testing123:testing123@test_bucket")

    with storage.fs(create=True) as fs:
        url = fs.geturl("prefix--file.jsonl.gz")
        assert url.startswith(
            "https://s3.amazonaws.com/test_bucket/prefix--file.jsonl.gz",
        )


@pytest.mark.parametrize(
    "file_url,root",
    [
        pytest.param(
            "file:///Users/sdk/path/to/file",
            "file:///Users/sdk/path/to",
            id="local",
        ),
        pytest.param(
            "s3://test_bucket/prefix--file.jsonl.gz",
            "s3://test_bucket",
            id="s3",
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


def test_json_lines_batcher():
    batcher = JSONLinesBatcher(
        "tap-test",
        "stream-test",
        batch_config=BatchConfig(
            encoding=JSONLinesEncoding("gzip"),
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


@pytest.mark.parametrize(
    "compression",
    [
        pytest.param("gzip", id="gzip"),
        pytest.param("none", id="none"),
    ],
)
def test_csv_batches(compression: str):
    batch_config = BatchConfig(
        encoding=CSVEncoding(compression=compression),
        storage=StorageTarget("file:///tmp/sdk-batches"),
        batch_size=2,
    )
    tap = BatchedTap()
    streams = tap.streams
    batches = list(streams["custom_batches"].get_batches(batch_config=batch_config))
    assert len(batches) == 2


def test_csv_batches_unsupported_format():
    batch_config = BatchConfig(
        encoding=CSVEncoding(compression="unknown"),
        storage=StorageTarget("file:///tmp/sdk-batches"),
        batch_size=2,
    )
    tap = BatchedTap()
    streams = tap.streams
    with pytest.raises(UnsupportedBatchCompressionError):
        list(streams["custom_batches"].get_batches(batch_config=batch_config))


def test_jsonl_batches_unsupported_format():
    batch_config = BatchConfig(
        encoding=CSVEncoding(compression="none"),
        storage=StorageTarget("file:///tmp/sdk-batches"),
        batch_size=2,
    )
    tap = BatchedTap()
    streams = tap.streams
    with pytest.raises(UnsupportedBatchFormatError):
        list(streams["default_batches"].get_batches(batch_config=batch_config))
