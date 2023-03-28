from __future__ import annotations

import pytest

from singer_sdk._singerlib import SingerMessageType
from singer_sdk.helpers._batch import JSONLinesEncoding, SDKBatchMessage


@pytest.mark.parametrize(
    "message,expected",
    [
        (
            SDKBatchMessage(
                stream="test_stream",
                encoding=JSONLinesEncoding("gzip"),
                manifest=[
                    "path/to/file1.jsonl.gz",
                    "path/to/file2.jsonl.gz",
                ],
            ),
            {
                "type": SingerMessageType.BATCH,
                "stream": "test_stream",
                "encoding": {"compression": "gzip", "format": "jsonl"},
                "manifest": [
                    "path/to/file1.jsonl.gz",
                    "path/to/file2.jsonl.gz",
                ],
            },
        ),
    ],
    ids=["batch-message-jsonl"],
)
def test_batch_message_as_dict(message, expected):
    """Test batch message as dict."""

    dumped = message.to_dict()
    assert dumped == expected

    assert message.from_dict(dumped) == message
