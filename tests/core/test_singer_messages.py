from dataclasses import asdict

import pytest

from singer_sdk.helpers._batch import JSONLinesEncoding, SDKBatchMessage
from singer_sdk.helpers._singer import SingerMessageType


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
        )
    ],
    ids=["batch-message-jsonl"],
)
def test_batch_message_as_dict(message, expected):
    """Test batch message as dict."""

    dumped = message.asdict()
    assert dumped == expected

    assert message.from_dict(dumped) == message
