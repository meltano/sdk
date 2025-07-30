"""{{ cookiecutter.destination_name }} target sink class, which handles writing streams."""

from __future__ import annotations

from singer_sdk.sinks import RecordSink


class {{ cookiecutter.destination_name }}Sink(RecordSink):
    """{{ cookiecutter.destination_name }} target sink class."""

    def process_record(self, record: dict, context: dict) -> None:
        """Process the record.

        Args:
            record: Individual record in the stream.
            context: Stream partition or context dictionary.
        """
        # Sample:
        # ------
        # client.write(record)  # noqa: ERA001
