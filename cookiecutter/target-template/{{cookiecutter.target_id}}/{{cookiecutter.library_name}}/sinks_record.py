"""{{ cookiecutter.destination_name }} target sink class, which handles writing streams."""

from __future__ import annotations

import sys

from singer_sdk.sinks import RecordSink

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override


class {{ cookiecutter.destination_name }}Sink(RecordSink):
    """{{ cookiecutter.destination_name }} target sink class."""

    @override
    def process_record(self, record: dict, context: dict) -> None:
        """Process the record.

        Args:
            record: Individual record in the stream.
            context: Stream partition or context dictionary.
        """
        # Sample:
        # ------
        # client.write(record)
