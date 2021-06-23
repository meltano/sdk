"""Sample Parquet target stream class, which handles writing streams."""

from typing import Any, Dict, List, Tuple, Union

{% set sinkclass = "BatchSink" if cookiecutter.serialization_method == "Per batch" else "RecordSink" %}
from singer_sdk.sinks import {{ sinkclass }}


class {{ cookiecutter.destination_name }}Sink({{ sinkclass }}):
    """{{ cookiecutter.destination_name }} target sink class."""

    {% if sinkclass == "RecordSink" -%}
    def process_record(self, record: dict, context: dict) -> None:
        """Process the record."""
        # Sample:
        # ------
        # client.write(record)
    {%- else -%}
    DEFAULT_BATCH_SIZE_ROWS = 10000

    def start_batch(self, context: dict) -> None:
        """Start a batch.
        
        Developers may optionally add additional markers to the `context` dict,
        which is unique to this batch.
        """
        # Sample:
        # ------
        # batch_key = context["batch_id"]
        # context["file_path"] = f"{batch_key}.csv"

    def process_record(self, record: dict, context: dict) -> None:
        """Process the record.

        Developers may optionally read or write additional markers within the
        passed `context` dict from the current batch.
        """
        # Sample:
        # ------
        # with open(context["file_path"], "a") as csvfile:
        #     csvfile.write(record)

    def process_batch(self, context: dict) -> None:
        """Write out any prepped records and return once fully written."""
        # Sample:
        # ------
        # client.upload(context["file_path"])  # Upload file
        # Path(context["file_path"]).unlink()  # Delete local copy
    {%- endif %}
