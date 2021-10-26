"""{{ cookiecutter.destination_name }} target sink class, which handles writing streams."""

{% set sinkclass = "BatchSink" if cookiecutter.serialization_method == "Per batch" else "RecordSink" %}
from singer_sdk.sinks import {{ sinkclass }}


class {{ cookiecutter.destination_name }}Sink({{ sinkclass }}):
    """{{ cookiecutter.destination_name }} target sink class."""
    {%- if sinkclass != "RecordSink" %}
    max_size = 10000  # Max records to write in one batch
    {%- endif %}

    def prepare_sink(self) -> None:
        """Set up the sink before running.

        This method is called before any messages are processed. It can be used
        to configure the sink, open connections, etc.
        """
        pass  # TODO: Delete this method if not needed.

    {% if sinkclass == "RecordSink" -%}
    def process_record(self, record: dict, context: dict) -> None:
        """Process the record."""
        # Sample:
        # ------
        # client.write(record)
    {%- else -%}
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

    def cleanup_sink(self) -> None:
        """Clean up resources after running.

        This method is called at the end of processing messages, including
        after exceptions are thrown. It can be used to clean up resources
        opened during `prepare_sink` such as connections.
        """
        pass  # TODO: Delete this method if not needed.
