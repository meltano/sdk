"""Unit tests for singer_sdk.streams._result."""

from __future__ import annotations

import logging

import pytest

from singer_sdk.streams._result import SyncResult, log_sync_result


def test_sync_result_combine(subtests: pytest.Subtests) -> None:
    """Test SyncResult.combine with all combinations of SyncResult values."""
    result_none = None
    with subtests.test("None returns the other result"):
        assert all(
            SyncResult.combine(result_none, other_result) is other_result
            for other_result in SyncResult
        )

    with subtests.test("FAILED beats all"):
        result = SyncResult.FAILED
        assert all(
            SyncResult.combine(result, other) is SyncResult.FAILED
            for other in SyncResult
        )
        assert all(
            SyncResult.combine(other, result) is SyncResult.FAILED
            for other in SyncResult
        )

    with subtests.test("ABORTED beats SUCCESS and PARTIAL"):
        result = SyncResult.ABORTED
        assert all(
            SyncResult.combine(result, other) is SyncResult.ABORTED
            for other in [SyncResult.SUCCESS, SyncResult.PARTIAL]
        )
        assert all(
            SyncResult.combine(other, result) is SyncResult.ABORTED
            for other in [SyncResult.SUCCESS, SyncResult.PARTIAL]
        )

    with subtests.test("PARTIAL beats SUCCESS"):
        result = SyncResult.PARTIAL
        assert SyncResult.combine(result, SyncResult.SUCCESS) is SyncResult.PARTIAL
        assert SyncResult.combine(SyncResult.SUCCESS, result) is SyncResult.PARTIAL

    with subtests.test("Idempotent combinations"):
        assert all(
            SyncResult.combine(any_result, any_result) is any_result
            for any_result in SyncResult
        )


def test_none_result_does_not_call_logger(caplog: pytest.LogCaptureFixture) -> None:
    logger = logging.getLogger("test_logger")
    with caplog.at_level("INFO", logger="test_logger"):
        log_sync_result(logger, "my_stream", None)

    assert len(caplog.records) == 0


@pytest.mark.parametrize(
    "result, expected_level",
    [
        (SyncResult.SUCCESS, logging.INFO),
        (SyncResult.FAILED, logging.ERROR),
        (SyncResult.ABORTED, logging.WARNING),
        (SyncResult.PARTIAL, logging.WARNING),
    ],
)
def test_log_result(
    result: SyncResult,
    caplog: pytest.LogCaptureFixture,
    expected_level: int,
) -> None:
    logger = logging.getLogger("test_logger")
    with caplog.at_level("INFO", logger="test_logger"):
        log_sync_result(logger, "my_stream", result)

    assert len(caplog.records) == 1

    record = caplog.records[0]
    assert record.name == "test_logger"
    assert record.levelno == expected_level
    assert record.getMessage() == f"Stream 'my_stream' sync result: {result.value}"
