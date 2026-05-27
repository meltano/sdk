"""A sample implementation for BigQuery."""

from __future__ import annotations

import typing as t

from singer_sdk import SQLConnector, SQLStream, SQLTap
from singer_sdk import typing as th  # JSON schema typing helpers

if t.TYPE_CHECKING:
    from collections.abc import Mapping


class BigQueryConnector(SQLConnector):
    """Connects to the BigQuery SQL source."""

    def get_sqlalchemy_url(self, config: Mapping) -> str:  # noqa: PLR6301
        """Concatenate a SQLAlchemy URL for use in connecting to the source."""
        return f"bigquery://{config['project_id']}"


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
