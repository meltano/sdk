"""Sink classes load data to a target."""

from __future__ import annotations

import abc
import sys
import typing as t

from singer_sdk.sinks.core import Sink

if sys.version_info >= (3, 12):
    from typing import override  # noqa: ICN003
else:
    from typing_extensions import override


class RecordSink(Sink):
    """Base class for singleton record writers."""

    current_size = 0  # Records are always written directly

    @override
    def _after_process_record(self, context: dict) -> None:
        """Perform post-processing and record keeping. Internal hook.

        The RecordSink class uses this method to tally each record written.

        Args:
            context: Stream partition or context dictionary.
        """
        self.tally_record_written()

    @t.final
    @override
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

    @override
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
