"""A sample implementation for BigQuery."""

from __future__ import annotations

from singer_sdk import SQLConnector, SQLStream, SQLTap
from singer_sdk import typing as th  # JSON schema typing helpers


class BigQueryConnector(SQLConnector):
    """Connects to the BigQuery SQL source."""

    def get_sqlalchemy_url(self, config: dict) -> str:  # noqa: PLR6301
        """Concatenate a SQLAlchemy URL for use in connecting to the source."""
        return f"bigquery://{config['project_id']}"

    def get_object_names(
        self,
        engine,
        inspected,
        schema_name: str,
    ) -> list[tuple[str, bool]]:
        """Return discoverable object names."""
        # Bigquery inspections returns table names in the form
        # `schema_name.table_name` which later results in the project name
        # override due to specifics in behavior of sqlalchemy-bigquery
        #
        # Let's strip `schema_name` prefix on the inspection

        return [
            (table_name.split(".")[-1], is_view)
            for (table_name, is_view) in super().get_object_names(
                engine,
                inspected,
                schema_name,
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
            "project_id",
            th.StringType,
            required=True,
            title="Project ID",
            description="GCP Project",
        ),
    ).to_dict()

    default_stream_class: type[SQLStream] = BigQueryStream


__all__ = ["BigQueryConnector", "BigQueryStream", "TapBigQuery"]
