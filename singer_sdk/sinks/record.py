"""Sink classes load data to a target."""

from __future__ import annotations

import abc

from singer_sdk.helpers._compat import final
from singer_sdk.sinks.core import Sink


class RecordSink(Sink):
    """Base class for singleton record writers."""

    current_size = 0  # Records are always written directly

    def _after_process_record(self, context: dict) -> None:  # noqa: ARG002
        """Perform post-processing and record keeping. Internal hook.

        The RecordSink class uses this method to tally each record written.

        Args:
            context: Stream partition or context dictionary.
        """
        self.tally_record_written()

    @final
    def process_batch(self, context: dict) -> None:
        """Do nothing and return immediately.

        The RecordSink class does not support batching.

        This method may not be overridden.

        Args:
            context: Stream partition or context dictionary.
        """

    @final
    def start_batch(self, context: dict) -> None:
        """Do nothing and return immediately.

        The RecordSink class does not support batching.

        This method may not be overridden.

        Args:
            context: Stream partition or context dictionary.
        """

    @abc.abstractmethod
    def process_record(self, record: dict, context: dict) -> None:
        """Load the latest record from the stream.

        This method must be overridden.

        Implementations should permanently serialize each record to the target
        prior to returning.

        If duplicates are merged/skipped instead of being loaded, merges can be
        tracked via :meth:`~singer_sdk.Sink.tally_duplicate_merged()`.

        Args:
            record: Individual record in the stream.
            context: Stream partition or context dictionary.
        """
