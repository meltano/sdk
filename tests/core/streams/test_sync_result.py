"""Unit tests for singer_sdk.streams._result."""

from __future__ import annotations

import logging

import pytest

from singer_sdk.streams._result import SyncResult, log_sync_result


def test_sync_result_combine(subtests: pytest.Subtests) -> None:
    """Test SyncResult.combine with all combinations of SyncResult values."""
    result_none = None
    with subtests.test("None returns the other result"):
        assert all(result.combine(result_none) is result for result in SyncResult)

    with subtests.test("FAILED beats all"):
        failed = SyncResult.FAILED
        assert all(result.combine(failed) is SyncResult.FAILED for result in SyncResult)
        assert all(failed.combine(result) is SyncResult.FAILED for result in SyncResult)

    with subtests.test("ABORTED beats SUCCESS and PARTIAL"):
        aborted = SyncResult.ABORTED
        assert all(
            result.combine(aborted) is SyncResult.ABORTED
            for result in [SyncResult.SUCCESS, SyncResult.PARTIAL]
        )
        assert all(
            aborted.combine(result) is SyncResult.ABORTED
            for result in [SyncResult.SUCCESS, SyncResult.PARTIAL]
        )

    with subtests.test("PARTIAL beats SUCCESS"):
        partial = SyncResult.PARTIAL
        assert SyncResult.SUCCESS.combine(partial) is SyncResult.PARTIAL
        assert partial.combine(SyncResult.SUCCESS) is SyncResult.PARTIAL

    with subtests.test("Idempotent combinations"):
        assert all(result.combine(result) is result for result in SyncResult)


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
