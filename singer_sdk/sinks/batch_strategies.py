"""Batch accumulation strategies for sink operations.

This module implements the Strategy pattern for batch management, allowing
different batching behaviors to be plugged in without modifying sink code.
This follows the Open/Closed Principle (OCP) - open for extension, closed
for modification.
"""

from __future__ import annotations

import abc
import datetime
import uuid

from singer_sdk.sinks.batch_context import BatchContext


class BatchStrategy(abc.ABC):
    """Abstract base class for batch accumulation strategies.

    A batch strategy determines:
    1. When to create a new batch
    2. When a batch should be flushed (loaded)
    3. How to add records to the current batch

    This abstraction separates batching logic from loading logic,
    following the Single Responsibility Principle.
    """

    def __init__(self) -> None:
        """Initialize the batch strategy."""
        self._current_batch: BatchContext | None = None

    @property
    def current_batch(self) -> BatchContext | None:
        """Get the current active batch, if any.

        Returns:
            The current batch or None if no batch is active.
        """
        return self._current_batch

    @abc.abstractmethod
    def should_flush(self) -> bool:
        """Determine if the current batch should be flushed.

        Returns:
            True if the batch should be flushed and loaded.
        """
        ...

    def add_record(self, record: dict) -> None:
        """Add a record to the current batch.

        If no batch exists, one will be created automatically.

        Args:
            record: The record to add to the batch.
        """
        if self._current_batch is None:
            self._current_batch = self._create_new_batch()

        self._current_batch.add_record(record)

    def get_batch(self) -> BatchContext:
        """Get the current batch and reset state.

        Returns:
            The current batch.

        Raises:
            RuntimeError: If no batch is currently active.
        """
        if self._current_batch is None:
            msg = "No batch is currently active"
            raise RuntimeError(msg)

        batch = self._current_batch
        self._current_batch = None
        return batch

    def _create_new_batch(self) -> BatchContext:
        """Create a new batch with default values.

        Returns:
            A new BatchContext instance.
        """
        return BatchContext(
            batch_id=str(uuid.uuid4()),
            batch_start_time=datetime.datetime.now(tz=datetime.timezone.utc),
        )


class NoBatchStrategy(BatchStrategy):
    """Strategy for sinks that don't batch (immediate loading).

    This strategy never accumulates records - it always reports that
    the batch should be flushed immediately. This is used for RecordSink
    implementations where each record loads immediately.
    """

    def should_flush(self) -> bool:
        """Always flush immediately.

        Returns:
            Always True.
        """
        return True


class RecordCountBatchStrategy(BatchStrategy):
    """Strategy that batches based on record count.

    This is the most common batching strategy - accumulate records until
    a maximum count is reached, then flush.

    Args:
        max_size: Maximum number of records before flushing (default: 1000).
    """

    def __init__(self, max_size: int = 1000) -> None:
        """Initialize with maximum batch size.

        Args:
            max_size: Maximum number of records before flushing.
        """
        super().__init__()
        self.max_size = max_size

    def should_flush(self) -> bool:
        """Flush when record count reaches max_size.

        Returns:
            True if current batch has reached max_size.
        """
        if self._current_batch is None:
            return False

        return self._current_batch.record_count() >= self.max_size


class TimeWindowBatchStrategy(BatchStrategy):
    """Strategy that batches based on time windows.

    Flush batches after a specified time window has elapsed since the
    batch was started.

    Args:
        window_seconds: Time window in seconds (default: 60).
    """

    def __init__(self, window_seconds: float = 60.0) -> None:
        """Initialize with time window.

        Args:
            window_seconds: Time window in seconds before flushing.
        """
        super().__init__()
        self.window_seconds = window_seconds

    def should_flush(self) -> bool:
        """Flush when time window has elapsed.

        Returns:
            True if the time window has elapsed since batch start.
        """
        if self._current_batch is None:
            return False

        elapsed = (
            datetime.datetime.now(tz=datetime.timezone.utc)
            - self._current_batch.batch_start_time
        ).total_seconds()

        return elapsed >= self.window_seconds


class HybridBatchStrategy(BatchStrategy):
    """Strategy that combines record count and time window.

    Flush when either the record count OR time window threshold is met,
    whichever comes first.

    Args:
        max_size: Maximum number of records before flushing (default: 1000).
        window_seconds: Time window in seconds (default: 60).
    """

    def __init__(
        self,
        max_size: int = 1000,
        window_seconds: float = 60.0,
    ) -> None:
        """Initialize with both count and time thresholds.

        Args:
            max_size: Maximum number of records before flushing.
            window_seconds: Time window in seconds before flushing.
        """
        super().__init__()
        self.count_strategy = RecordCountBatchStrategy(max_size)
        self.time_strategy = TimeWindowBatchStrategy(window_seconds)

    @property
    def current_batch(self) -> BatchContext | None:
        """Get the current active batch.

        Returns:
            The current batch or None.
        """
        return self._current_batch

    @current_batch.setter
    def current_batch(self, batch: BatchContext | None) -> None:
        """Set the current batch for all sub-strategies.

        Args:
            batch: The batch to set.
        """
        self._current_batch = batch
        self.count_strategy._current_batch = batch
        self.time_strategy._current_batch = batch

    def add_record(self, record: dict) -> None:
        """Add a record and sync state across sub-strategies.

        Args:
            record: The record to add.
        """
        super().add_record(record)
        # Sync the batch reference to sub-strategies
        self.count_strategy._current_batch = self._current_batch
        self.time_strategy._current_batch = self._current_batch

    def should_flush(self) -> bool:
        """Flush when either count or time threshold is met.

        Returns:
            True if either strategy says to flush.
        """
        return self.count_strategy.should_flush() or self.time_strategy.should_flush()

    def get_batch(self) -> BatchContext:
        """Get batch and reset state for all sub-strategies.

        Returns:
            The current batch.
        """
        batch = super().get_batch()
        self.count_strategy._current_batch = None
        self.time_strategy._current_batch = None
        return batch


class SizeBatchStrategy(BatchStrategy):
    """Strategy that batches based on estimated memory size.

    This is useful for preventing out-of-memory errors when dealing with
    large records.

    Args:
        max_size_bytes: Maximum estimated size in bytes before flushing.

    Note:
        Size estimation is approximate and based on record count * average size.
    """

    def __init__(self, max_size_bytes: int = 10 * 1024 * 1024) -> None:  # 10 MB default
        """Initialize with maximum size.

        Args:
            max_size_bytes: Maximum estimated size in bytes before flushing.
        """
        super().__init__()
        self.max_size_bytes = max_size_bytes
        self._total_size = 0
        self._record_count = 0

    def add_record(self, record: dict) -> None:
        """Add a record and track size.

        Args:
            record: The record to add.
        """
        super().add_record(record)
        # Rough estimate: average size per record
        # For better accuracy, implement size estimation in your sink
        self._record_count += 1
        if self._record_count == 1:
            # Estimate first record size (very rough)
            self._total_size = len(str(record))
        else:
            # Use running average
            avg_size = self._total_size / (self._record_count - 1)
            self._total_size += int(avg_size)

    def should_flush(self) -> bool:
        """Flush when estimated size reaches max_size_bytes.

        Returns:
            True if estimated size has reached the threshold.
        """
        if self._current_batch is None:
            return False

        return self._total_size >= self.max_size_bytes

    def get_batch(self) -> BatchContext:
        """Get batch and reset size tracking.

        Returns:
            The current batch.
        """
        batch = super().get_batch()
        self._total_size = 0
        self._record_count = 0
        return batch
