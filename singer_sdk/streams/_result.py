"""Sync outcome tracking for Singer streams."""

from __future__ import annotations

import enum
import logging


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

    def combine(self, other: SyncResult | None) -> SyncResult:
        """Merge this result with a prior accumulated result.

        ``self`` is the new outcome being applied; ``other`` is whatever has
        accumulated so far (``None`` means no prior result, so ``self`` wins
        unconditionally).  Priority order: FAILED > ABORTED > PARTIAL > SUCCESS.

        Args:
            other: The accumulated result so far, or None if this is the first.

        Returns:
            The combined SyncResult.
        """
        if other is None:
            return self

        if self is SyncResult.FAILED or other is SyncResult.FAILED:
            return SyncResult.FAILED

        if self is SyncResult.ABORTED or other is SyncResult.ABORTED:
            return SyncResult.ABORTED

        if self is SyncResult.PARTIAL or other is SyncResult.PARTIAL:
            return SyncResult.PARTIAL

        return SyncResult.SUCCESS


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
