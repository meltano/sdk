"""Sample Parquet target stream class, which handles writing streams."""

from typing import Any, Dict, List, Tuple, Union

{% set sinkclass = "BatchSink" if cookiecutter.serialization_method == "Per batch" else "RecordSink" %}
from singer_sdk.sinks import {{ sinkclass }}


class {{ cookiecutter.destination_name }}Sink({{ sinkclass }}):
    """{{ cookiecutter.destination_name }} target sink class."""
    {% if sinkclass == "BatchSink" %}
    DEFAULT_BATCH_SIZE_ROWS = 10000
    {%- endif %}

    def process_record(self, record: dict) -> None:
        """Process the record."""
        # Sample:
        # for record in records_to_drain:
        #     write(record)
        #     count += 1
        # self.tally_record_written(count)

    {% if sinkclass == "BatchSink" -%}
    def process_batch(self, records_to_drain: Union[List[dict], Any]) -> None:
        """Write out any prepped records and return once fully written."""
        # Sample:
        # for record in records_to_drain:
        #     write(record)
        #     count += 1
        # self.tally_record_written(count)
    {%- endif %}
