"""Sink classes load data to a target."""

import abc
import datetime
import uuid

from singer_sdk.sinks.core import Sink


class BatchSink(Sink):
    """Base class for batched record writers."""

    def _get_context(self, record: dict) -> dict:
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
            new_context = {
                "batch_id": str(uuid.uuid4()),
                "batch_start_time": datetime.datetime.now(),
            }
            self.start_batch(new_context)
            self._pending_batch = new_context

        return self._pending_batch

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
        pass

    def process_record(self, record: dict, context: dict) -> None:
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

    @abc.abstractmethod
    def process_batch(self, context: dict) -> None:
        """Process a batch with the given batch context.

        This method must be overridden.

        If :meth:`~singer_sdk.BatchSink.process_record()` is not overridden,
        the `context["records"]` list will contain all records from the given batch
        context.

        If duplicates are merged, these can be tracked via
        :meth:`~singer_sdk.Sink.tally_duplicate_merged()`.

        Args:
            context: Stream partition or context dictionary.
        """
        pass
