"""Protocol definitions for sink loading operations.

These protocols define clean, SOLID-compliant interfaces for loading data.
They are designed to work alongside the existing Sink architecture while
providing a clearer, more maintainable path forward.
"""

from __future__ import annotations

import typing as t
from typing import Protocol

if t.TYPE_CHECKING:
    from singer_sdk.sinks.batch_context import BatchContext


class IRecordLoader(Protocol):
    """Protocol for sinks that load records one at a time.

    This protocol represents immediate, per-record loading where each record
    is permanently written to the target as soon as it's received.

    Example:
        class MyAPISink(RecordLoadingSink):
            def load_record(self, record: dict) -> None:
                self.api_client.post("/endpoint", json=record)
    """

    def load_record(self, record: dict) -> None:
        """Load a single record immediately to the target.

        Args:
            record: The record to load.
        """
        ...


class IBatchLoader(Protocol):
    """Protocol for sinks that load records in batches.

    This protocol represents batched loading where records are accumulated
    and then loaded together in bulk operations.

    Example:
        class MyBulkSink(BatchLoadingSink):
            def load_batch(self, batch: BatchContext) -> None:
                self.db.bulk_insert(batch.records)
    """

    def load_batch(self, batch: BatchContext) -> None:
        """Load an accumulated batch of records to the target.

        Args:
            batch: The batch context containing records and metadata.
        """
        ...


class IPreprocessable(Protocol):
    """Protocol for sinks that need to preprocess records before loading.

    This is optional and allows sinks to transform records before they
    are validated and loaded.
    """

    def preprocess_record(self, record: dict) -> dict:
        """Preprocess a record before validation and loading.

        Args:
            record: The raw record from the stream.

        Returns:
            The preprocessed record.
        """
        ...


class IVersionable(Protocol):
    """Protocol for sinks that support version activation.

    Version activation is used for targets that support ACTIVATE_VERSION
    messages, typically for maintaining historical data or handling
    slowly changing dimensions.
    """

    def activate_version(self, new_version: int) -> None:
        """Activate a new version in the target.

        Args:
            new_version: The version number to activate.
        """
        ...


class IBatchable(Protocol):
    """Protocol for sinks that support batch lifecycle hooks.

    This allows sinks to perform setup and teardown operations around
    batch processing.
    """

    def start_batch(self, batch: BatchContext) -> None:
        """Called when a new batch is started.

        Args:
            batch: The batch context that was just created.
        """
        ...

    def end_batch(self, batch: BatchContext) -> None:
        """Called when a batch is finished loading.

        Args:
            batch: The batch context that was just loaded.
        """
        ...
