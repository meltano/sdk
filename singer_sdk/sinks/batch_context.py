"""Type-safe batch context for sink operations.

This module provides strongly-typed alternatives to the generic dict-based
context used in legacy sink implementations.
"""

from __future__ import annotations

import datetime
import typing as t
from dataclasses import dataclass, field


@dataclass
class BatchContext:
    """Type-safe batch context for batch loading operations.

    This replaces the legacy dict-based context with a strongly-typed,
    well-documented structure that provides IDE autocomplete and type safety.

    Attributes:
        batch_id: Unique identifier for this batch (UUID string).
        batch_start_time: Timestamp when the batch was started.
        records: List of records accumulated in this batch.
        metadata: Additional custom metadata that can be added by sink implementations.

    Example:
        class MyBatchSink(BatchLoadingSink):
            def start_batch(self, batch: BatchContext) -> None:
                batch.metadata["temp_file"] = f"/tmp/{batch.batch_id}.jsonl"

            def load_batch(self, batch: BatchContext) -> None:
                temp_file = batch.metadata["temp_file"]
                with open(temp_file) as f:
                    self.api.upload(f)
    """

    batch_id: str
    batch_start_time: datetime.datetime
    records: list[dict] = field(default_factory=list)
    metadata: dict[str, t.Any] = field(default_factory=dict)

    def add_record(self, record: dict) -> None:
        """Add a record to this batch.

        Args:
            record: The record to add.
        """
        self.records.append(record)

    def record_count(self) -> int:
        """Get the number of records in this batch.

        Returns:
            The number of records.
        """
        return len(self.records)

    def clear_records(self) -> None:
        """Clear all records from this batch.

        This is useful for resetting batch state after loading.
        """
        self.records.clear()

    def to_legacy_dict(self) -> dict:
        """Convert to legacy dict format for backward compatibility.

        This allows new BatchContext objects to work with code that expects
        the old dict-based context format.

        Returns:
            A dict containing batch_id, batch_start_time, records, and metadata.
        """
        return {
            "batch_id": self.batch_id,
            "batch_start_time": self.batch_start_time,
            "records": self.records,
            **self.metadata,
        }

    @classmethod
    def from_legacy_dict(cls, context: dict) -> BatchContext:
        """Create a BatchContext from legacy dict format.

        This allows old dict-based contexts to be converted to the new
        BatchContext format.

        Args:
            context: The legacy dict-based context.

        Returns:
            A new BatchContext instance.
        """
        # Extract known fields
        batch_id = context.get("batch_id", "")
        batch_start_time = context.get(
            "batch_start_time",
            datetime.datetime.now(tz=datetime.timezone.utc),
        )
        records = context.get("records", [])

        # Everything else goes into metadata
        metadata = {
            k: v
            for k, v in context.items()
            if k not in {"batch_id", "batch_start_time", "records"}
        }

        return cls(
            batch_id=batch_id,
            batch_start_time=batch_start_time,
            records=records,
            metadata=metadata,
        )
