"""Unit tests for singer_sdk.streams._result."""

from __future__ import annotations

import logging
import unittest.mock

import pytest

from singer_sdk.streams._result import SyncResult, log_sync_result


class TestSyncResultCombine:
    """Tests for SyncResult.combine."""

    @pytest.mark.parametrize(
        "result2",
        [SyncResult.SUCCESS, SyncResult.FAILED, SyncResult.ABORTED, SyncResult.PARTIAL],
    )
    def test_none_returns_result2(self, result2: SyncResult) -> None:
        assert SyncResult.combine(None, result2) is result2

    @pytest.mark.parametrize(
        "other",
        [SyncResult.SUCCESS, SyncResult.FAILED, SyncResult.ABORTED, SyncResult.PARTIAL],
    )
    def test_failed_beats_all(self, other: SyncResult) -> None:
        assert SyncResult.combine(SyncResult.FAILED, other) is SyncResult.FAILED
        assert SyncResult.combine(other, SyncResult.FAILED) is SyncResult.FAILED

    @pytest.mark.parametrize(
        "other",
        [SyncResult.SUCCESS, SyncResult.PARTIAL],
    )
    def test_aborted_beats_success_and_partial(self, other: SyncResult) -> None:
        assert SyncResult.combine(SyncResult.ABORTED, other) is SyncResult.ABORTED
        assert SyncResult.combine(other, SyncResult.ABORTED) is SyncResult.ABORTED

    def test_aborted_loses_to_failed(self) -> None:
        assert (
            SyncResult.combine(SyncResult.ABORTED, SyncResult.FAILED)
            is SyncResult.FAILED
        )
        assert (
            SyncResult.combine(SyncResult.FAILED, SyncResult.ABORTED)
            is SyncResult.FAILED
        )

    def test_partial_beats_success(self) -> None:
        assert (
            SyncResult.combine(SyncResult.PARTIAL, SyncResult.SUCCESS)
            is SyncResult.PARTIAL
        )
        assert (
            SyncResult.combine(SyncResult.SUCCESS, SyncResult.PARTIAL)
            is SyncResult.PARTIAL
        )

    def test_partial_loses_to_failed_and_aborted(self) -> None:
        assert (
            SyncResult.combine(SyncResult.PARTIAL, SyncResult.FAILED)
            is SyncResult.FAILED
        )
        assert (
            SyncResult.combine(SyncResult.PARTIAL, SyncResult.ABORTED)
            is SyncResult.ABORTED
        )

    @pytest.mark.parametrize(
        "value",
        [SyncResult.SUCCESS, SyncResult.FAILED, SyncResult.ABORTED, SyncResult.PARTIAL],
    )
    def test_same_value_is_idempotent(self, value: SyncResult) -> None:
        assert SyncResult.combine(value, value) is value


class TestLogSyncResult:
    """Tests for log_sync_result."""

    def test_none_result_does_not_call_logger(self) -> None:
        logger = unittest.mock.MagicMock(spec=logging.Logger)
        log_sync_result(logger, "my_stream", None)
        logger.log.assert_not_called()

    @pytest.mark.parametrize(
        "result, expected_level",
        [
            (SyncResult.SUCCESS, logging.INFO),
            (SyncResult.FAILED, logging.ERROR),
            (SyncResult.ABORTED, logging.WARNING),
            (SyncResult.PARTIAL, logging.WARNING),
        ],
    )
    def test_correct_log_level(
        self,
        result: SyncResult,
        expected_level: int,
    ) -> None:
        logger = unittest.mock.MagicMock(spec=logging.Logger)
        log_sync_result(logger, "my_stream", result)
        logger.log.assert_called_once()
        call_args = logger.log.call_args
        assert call_args[0][0] == expected_level

    @pytest.mark.parametrize(
        "result",
        [SyncResult.SUCCESS, SyncResult.FAILED, SyncResult.ABORTED, SyncResult.PARTIAL],
    )
    def test_message_contains_stream_name_and_result(self, result: SyncResult) -> None:
        logger = unittest.mock.MagicMock(spec=logging.Logger)
        log_sync_result(logger, "my_stream", result)
        call_args = logger.log.call_args
        # The format string and positional args are passed to logger.log
        fmt = call_args[0][1]
        name_arg = call_args[0][2]
        value_arg = call_args[0][3]
        assert name_arg == "my_stream"
        assert result.value == value_arg
        assert "%s" in fmt
