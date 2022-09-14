"""A sample implementation for BigQuery."""

from typing import List, Tuple, Type

from singer_sdk import SQLConnector, SQLStream, SQLTap
from singer_sdk import typing as th  # JSON schema typing helpers


class BigQueryConnector(SQLConnector):
    """Connects to the BigQuery SQL source."""

    def get_sqlalchemy_url(cls, config: dict) -> str:
        """Concatenate a SQLAlchemy URL for use in connecting to the source."""
        return f"bigquery://{config['project_id']}"

    def get_object_names(
        self, engine, inspected, schema_name: str
    ) -> List[Tuple[str, bool]]:
        """Return discoverable object names."""
        # Bigquery inspections returns table names in the form
        # `schema_name.table_name` which later results in the project name
        # override due to specifics in behavior of sqlalchemy-bigquery
        #
        # Let's strip `schema_name` prefix on the inspection

        return [
            (table_name.split(".")[-1], is_view)
            for (table_name, is_view) in super().get_object_names(
                engine, inspected, schema_name
            )
        ]


class BigQueryStream(SQLStream):
    """Stream class for BigQuery streams."""

    connector_class = BigQueryConnector


class TapBigQuery(SQLTap):
    """BigQuery tap class."""

    name = "tap-bigquery"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "project_id", th.StringType, required=True, description="GCP Project"
        ),
    ).to_dict()

    default_stream_class: Type[SQLStream] = BigQueryStream


__all__ = ["TapBigQuery", "BigQueryConnector", "BigQueryStream"]
