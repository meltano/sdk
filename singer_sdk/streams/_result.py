"""Sync outcome tracking for Singer streams."""

from __future__ import annotations

import enum
import logging

__all__ = ["SyncResult", "log_sync_result"]


class SyncResult(enum.Enum):
    """Outcome of a single stream's sync operation.

    Set on :attr:`~singer_sdk.Stream.sync_result` after
    :meth:`~singer_sdk.Stream.sync` completes or fails.

    Attributes:
        SUCCESS: Completed without error.
        FAILED: Raised a fatal (non-lifecycle) exception.
        ABORTED: Raised a lifecycle abort exception
            (:class:`~singer_sdk.exceptions.AbortedSyncFailedException` or
            :class:`~singer_sdk.exceptions.AbortedSyncPausedException`).
        PARTIAL: Reserved — ignorable errors with skipped records (requires PR 3).
    """

    SUCCESS = "success"
    FAILED = "failed"
    ABORTED = "aborted"
    PARTIAL = "partial"

    @classmethod
    def combine(cls, result1: SyncResult | None, result2: SyncResult) -> SyncResult:
        """Merge two results; None means 'no prior result'.

        Priority order: FAILED > ABORTED > PARTIAL > SUCCESS.

        Args:
            result1: The first SyncResult, or None to treat as no prior result.
            result2: The second SyncResult.

        Returns:
            The combined SyncResult.
        """
        if result1 is None:
            return result2

        if result1 is cls.FAILED or result2 is cls.FAILED:
            return cls.FAILED

        if result1 is cls.ABORTED or result2 is cls.ABORTED:
            return cls.ABORTED

        if result1 is cls.PARTIAL or result2 is cls.PARTIAL:
            return cls.PARTIAL

        return cls.SUCCESS


_LEVEL_MAP: dict[SyncResult, int] = {
    SyncResult.SUCCESS: logging.INFO,
    SyncResult.FAILED: logging.ERROR,
    SyncResult.ABORTED: logging.WARNING,
    SyncResult.PARTIAL: logging.WARNING,
}


def log_sync_result(
    logger: logging.Logger,
    stream_name: str,
    result: SyncResult | None,
) -> None:
    """Log a one-line sync outcome. No-op when result is None.

    Args:
        logger: The logger to use.
        stream_name: The name of the stream.
        result: The sync result, or None if the stream was never synced.
    """
    if result is None:
        return
    logger.log(
        _LEVEL_MAP.get(result, logging.INFO),
        "Stream '%s' sync result: %s",
        stream_name,
        result.value,
    )
