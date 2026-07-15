"""Test BATCH message support."""

from __future__ import annotations

import singer
import singer.batch
import singer_sdk.helpers._batch as sdk_batch


def test_batch_message_round_trip():
    msg = singer.BatchMessage(
        stream="s",
        encoding={"format": "jsonl", "compression": "gzip"},  # type: ignore[arg-type]
        manifest=["file1.jsonl.gz", "file2.jsonl.gz"],
    )
    assert msg.type == "BATCH"
    assert isinstance(msg.encoding, singer.BaseBatchFileEncoding)
    assert msg.to_dict() == {
        "type": "BATCH",
        "stream": "s",
        "encoding": {"format": "jsonl", "compression": "gzip"},
        "manifest": ["file1.jsonl.gz", "file2.jsonl.gz"],
    }


def test_batch_message_arbitrary_string_format():
    """Encodings accept arbitrary format strings, e.g. "arrow"."""
    msg = singer.BatchMessage(
        stream="s",
        encoding=singer.BaseBatchFileEncoding(format="arrow"),
        manifest=["file1.arrow"],
    )
    assert msg.encoding.format == "arrow"
    assert msg.to_dict()["encoding"] == {"format": "arrow"}


def test_base_batch_file_encoding_from_dict():
    encoding = singer.batch.BaseBatchFileEncoding.from_dict({"format": "parquet"})
    assert encoding.format == "parquet"
    assert encoding.compression is None


def test_sdk_batch_message_is_alias():
    assert sdk_batch.SDKBatchMessage is singer.BatchMessage
    assert sdk_batch.BaseBatchFileEncoding is singer.BaseBatchFileEncoding
