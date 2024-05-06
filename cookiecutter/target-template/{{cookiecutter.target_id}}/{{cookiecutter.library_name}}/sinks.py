"""{{ cookiecutter.destination_name }} target sink class, which handles writing streams."""

from __future__ import annotations

{%- set
    sinkclass_mapping = {
        "Per batch": "BatchSink",
        "Per record": "RecordSink",
        "SQL": "SQLSink",
    }
%}

{%- set sinkclass = sinkclass_mapping[cookiecutter.serialization_method] %}

{%- if sinkclass == "SQLSink" %}

from singer_sdk.connectors import SQLConnector
from singer_sdk.sinks import {{ sinkclass }}
{%- else %}

from singer_sdk.sinks import {{ sinkclass }}
{%- endif %}


{%- if sinkclass == "SQLSink" %}


class {{ cookiecutter.destination_name }}Connector(SQLConnector):
    """The connector for {{ cookiecutter.destination_name }}.

    This class handles all DDL and type conversions.
    """

    allow_column_add: bool = True  # Whether ADD COLUMN is supported.
    allow_column_rename: bool = True  # Whether RENAME COLUMN is supported.
    allow_column_alter: bool = False  # Whether altering column types is supported.
    allow_merge_upsert: bool = False  # Whether MERGE UPSERT is supported.
    allow_overwrite: bool = False  # Whether overwrite load method is supported.
    allow_temp_tables: bool = True  # Whether temp tables are supported.

    def get_sqlalchemy_url(self, config: dict) -> str:
        """Generates a SQLAlchemy URL for {{ cookiecutter.destination_name }}.

        Args:
            config: The configuration for the connector.
        """
        return super().get_sqlalchemy_url(config)
{%- endif %}


class {{ cookiecutter.destination_name }}Sink({{ sinkclass }}):
    """{{ cookiecutter.destination_name }} target sink class."""

    {% if sinkclass == "RecordSink" -%}
    def process_record(self, record: dict, context: dict) -> None:
        """Process the record.

        Args:
            record: Individual record in the stream.
            context: Stream partition or context dictionary.
        """
        # Sample:
        # ------
        # client.write(record)  # noqa: ERA001

    {%- elif sinkclass == "BatchSink" -%}

    max_size = 10000  # Max records to write in one batch

    def start_batch(self, context: dict) -> None:
        """Start a batch.

        Developers may optionally add additional markers to the `context` dict,
        which is unique to this batch.

        Args:
            context: Stream partition or context dictionary.
        """
        # Sample:
        # ------
        # batch_key = context["batch_id"]
        # context["file_path"] = f"{batch_key}.csv"

    def process_record(self, record: dict, context: dict) -> None:
        """Process the record.

        Developers may optionally read or write additional markers within the
        passed `context` dict from the current batch.

        Args:
            record: Individual record in the stream.
            context: Stream partition or context dictionary.
        """
        # Sample:
        # ------
        # with open(context["file_path"], "a") as csvfile:
        #     csvfile.write(record)

    def process_batch(self, context: dict) -> None:
        """Write out any prepped records and return once fully written.

        Args:
            context: Stream partition or context dictionary.
        """
        # Sample:
        # ------
        # client.upload(context["file_path"])  # Upload file
        # Path(context["file_path"]).unlink()  # Delete local copy

    {%- elif sinkclass == "SQLSink" -%}

    connector_class = {{ cookiecutter.destination_name }}Connector
    {%- endif %}
