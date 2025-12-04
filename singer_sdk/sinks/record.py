"""Sink classes load data to a target."""

from __future__ import annotations

import typing as t

from singer_sdk.sinks.core import Sink


class RecordSink(Sink):
    """Base class for singleton record writers.

    This class supports both legacy and modern loading patterns:

    **Legacy pattern (deprecated but supported):**
        Override process_record(record: dict, context: dict) to load each record

    **Modern pattern (recommended):**
        Override load_record(record: dict) for cleaner, simpler immediate loading

    Example (modern pattern):
        class MyRecordSink(RecordSink):
            def load_record(self, record: dict) -> None:
                # Clear, simple loading
                self.api_client.post(record)

    Example (legacy pattern - still works):
        class MyLegacySink(RecordSink):
            def process_record(self, record: dict, context: dict) -> None:
                # Old style - context is unused for RecordSink
                self.api_client.post(record)
    """

    current_size = 0  # Records are always written directly

    def _after_process_record(self, context: dict) -> None:  # noqa: ARG002
        """Perform post-processing and record keeping. Internal hook.

        The RecordSink class uses this method to tally each record written.

        Args:
            context: Stream partition or context dictionary.
        """
        self.tally_record_written()

    @t.final
    def process_batch(self, context: dict) -> None:
        """Do nothing and return immediately.

        The RecordSink class does not support batching.

        This method may not be overridden.

        Args:
            context: Stream partition or context dictionary.
        """

    @t.final
    def start_batch(self, context: dict) -> None:
        """Do nothing and return immediately.

        The RecordSink class does not support batching.

        This method may not be overridden.

        Args:
            context: Stream partition or context dictionary.
        """

    def load_record(self, record: dict) -> None:
        """Load a single record immediately to the target.

        **Modern pattern (recommended):** Override this method for cleaner,
        simpler immediate record loading.

        This method provides a cleaner alternative to :meth:`process_record`.
        It takes only the record (no context dict), making the intent clear
        and reducing complexity.

        Example:
            class MyRecordSink(RecordSink):
                def load_record(self, record: dict) -> None:
                    # Simple, clear loading
                    self.api_client.post("/endpoint", json=record)

        Args:
            record: The record to load immediately.

        Note:
            If you override this method, you do NOT need to override
            :meth:`process_record`. The framework will automatically call
            this method instead.
        """
        # Default implementation: call legacy process_record
        # This maintains backward compatibility for subclasses that haven't
        # implemented load_record() yet
        self.process_record(record, {})

    def process_record(self, record: dict, context: dict) -> None:
        """Load the latest record from the stream.

        **Legacy pattern (deprecated but supported):** Override this method
        for dict-based record loading with context.

        **Modern pattern (recommended):** Override :meth:`load_record` instead
        for simpler, clearer immediate loading.

        If you override :meth:`load_record`, you do NOT need to override this
        method. The framework will automatically route to your :meth:`load_record`
        implementation.

        Implementations should permanently serialize each record to the target
        prior to returning.

        If duplicates are merged/skipped instead of being loaded, merges can be
        tracked via :meth:`~singer_sdk.Sink.tally_duplicate_merged()`.

        Args:
            record: Individual record in the stream.
            context: Stream partition or context dictionary.

        Raises:
            NotImplementedError: If neither process_record nor load_record is overridden.
        """
        # Check if subclass has overridden load_record() (the new way)
        if type(self).load_record is not RecordSink.load_record:
            # Subclass implemented load_record - use it!
            self.load_record(record)
        else:
            # Neither method was overridden - this is an error
            msg = (
                f"{self.__class__.__name__} must override either "
                f"load_record(record: dict) or "
                f"process_record(record: dict, context: dict). "
                f"The load_record() method is recommended for new implementations."
            )
            raise NotImplementedError(msg)
