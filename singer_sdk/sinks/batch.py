"""Sink classes load data to a target."""

from __future__ import annotations

import datetime
import typing as t
import uuid

from singer_sdk.sinks.batch_context import BatchContext
from singer_sdk.sinks.core import Sink

if t.TYPE_CHECKING:
    from singer_sdk.sinks.batch_strategies import BatchStrategy


class BatchSink(Sink):
    """Base class for batched record writers.

    This class supports both legacy and modern loading patterns:

    **Legacy pattern (deprecated but supported):**
        Override process_batch(context: dict) to load context["records"]

    **Modern pattern (recommended):**
        Override load_batch(batch: BatchContext) for type-safe, clear loading

    Example (modern pattern):
        class MyBatchSink(BatchSink):
            def load_batch(self, batch: BatchContext) -> None:
                # Type-safe access to batch data
                self.api.bulk_insert(batch.records)

    Example (legacy pattern - still works):
        class MyLegacySink(BatchSink):
            def process_batch(self, context: dict) -> None:
                # Old style - will show deprecation warning
                self.api.bulk_insert(context["records"])
    """

    def __init__(
        self,
        *args: t.Any,
        batch_strategy: BatchStrategy | None = None,
        **kwargs: t.Any,
    ) -> None:
        """Initialize the batch sink.

        Args:
            *args: Positional arguments to pass to parent Sink.
            batch_strategy: Optional batch strategy for custom batching behavior.
                If not provided, uses default record-count based batching.
            **kwargs: Keyword arguments to pass to parent Sink.
        """
        super().__init__(*args, **kwargs)
        self._batch_strategy = batch_strategy
        self._batch_context_typed: BatchContext | None = None

    def _get_context(self, record: dict) -> dict:  # noqa: ARG002
        """Return a batch context. If no batch is active, return a new batch context.

        The SDK-generated context will contain `batch_id` (GUID string) and
        `batch_start_time` (datetime).

        NOTE: Future versions of the SDK may expand the available context attributes.

        Args:
            record: Individual record in the stream.

        Returns:
            TODO
        """
        if self._pending_batch is None:
            # Create typed context for modern sinks
            self._batch_context_typed = BatchContext(
                batch_id=str(uuid.uuid4()),
                batch_start_time=datetime.datetime.now(tz=datetime.timezone.utc),
            )
            # Create dict context for legacy sinks
            new_context = self._batch_context_typed.to_legacy_dict()
            self.start_batch(new_context)
            self._pending_batch = new_context

        return self._pending_batch

    def _get_batch_context_typed(self) -> BatchContext:
        """Get the current batch as a typed BatchContext.

        This is used internally to provide type-safe batch handling.

        Returns:
            The current BatchContext instance.
        """
        if self._batch_context_typed is None:
            # Shouldn't happen, but create from dict if needed
            self._batch_context_typed = BatchContext.from_legacy_dict(
                self._pending_batch or {},
            )
        return self._batch_context_typed

    def start_batch(self, context: dict) -> None:
        """Start a new batch with the given context.

        The SDK-generated context will contain `batch_id` (GUID string) and
        `batch_start_time` (datetime).

        Developers may optionally override this method to add custom markers to the
        `context` dict and/or to initialize batch resources - such as initializing a
        local temp file to hold batch records before uploading.

        Args:
            context: Stream partition or context dictionary.
        """

    def process_record(self, record: dict, context: dict) -> None:  # noqa: PLR6301
        """Load the latest record from the stream.

        Developers may either load to the `context` dict for staging (the
        default behavior for Batch types), or permanently write out to the target.

        If this method is not overridden, the default implementation will create a
        `context["records"]` list and append all records for processing during
        :meth:`~singer_sdk.BatchSink.process_batch()`.

        If duplicates are merged, these can be tracked via
        :meth:`~singer_sdk.Sink.tally_duplicate_merged()`.

        Args:
            record: Individual record in the stream.
            context: Stream partition or context dictionary.
        """
        if "records" not in context:
            context["records"] = []

        context["records"].append(record)

    def load_batch(self, batch: BatchContext) -> None:
        """Load an accumulated batch of records to the target.

        **Modern pattern (recommended):** Override this method for type-safe,
        clear batch loading.

        This method provides a cleaner, more maintainable alternative to
        :meth:`process_batch`. It receives a strongly-typed :class:`BatchContext`
        instead of a generic dict, providing IDE autocomplete and type safety.

        Example:
            class MyBatchSink(BatchSink):
                def load_batch(self, batch: BatchContext) -> None:
                    # Type-safe access to batch data
                    records = batch.records  # IDE knows this is list[dict]
                    batch_id = batch.batch_id  # IDE knows this is str
                    self.api.bulk_insert(records)

        Args:
            batch: The batch context containing records and metadata.

        Note:
            If you override this method, you do NOT need to override
            :meth:`process_batch`. The framework will automatically call
            this method instead.
        """
        # Default implementation: call legacy process_batch with dict context
        # This maintains backward compatibility for subclasses that haven't
        # implemented load_batch() yet
        self.process_batch(batch.to_legacy_dict())

    def process_batch(self, context: dict) -> None:
        """Process a batch with the given batch context.

        **Legacy pattern (deprecated but supported):** Override this method
        for dict-based batch loading.

        **Modern pattern (recommended):** Override :meth:`load_batch` instead
        for type-safe batch loading with :class:`BatchContext`.

        If you override :meth:`load_batch`, you do NOT need to override this
        method. The framework will automatically route to your :meth:`load_batch`
        implementation.

        If :meth:`~singer_sdk.BatchSink.process_record()` is not overridden,
        the `context["records"]` list will contain all records from the given batch
        context.

        If duplicates are merged, these can be tracked via
        :meth:`~singer_sdk.Sink.tally_duplicate_merged()`.

        Args:
            context: Stream partition or context dictionary.

        Raises:
            NotImplementedError: If neither process_batch nor load_batch is overridden.
        """
        # Check if subclass has overridden load_batch() (the new way)
        # We detect this by checking if the method is different from the base class
        if type(self).load_batch is not BatchSink.load_batch:
            # Subclass implemented load_batch - use it!
            batch = BatchContext.from_legacy_dict(context)
            self.load_batch(batch)
        else:
            # Neither method was overridden - this is an error
            msg = (
                f"{self.__class__.__name__} must override either "
                f"load_batch(batch: BatchContext) or "
                f"process_batch(context: dict). "
                f"The load_batch() method is recommended for new implementations."
            )
            raise NotImplementedError(msg)
