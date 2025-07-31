"""{{ cookiecutter.destination_name }} target sink class, which handles writing streams."""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING, Any

from singer_sdk.connectors import SQLConnector
from singer_sdk.connectors.sql import FullyQualifiedName
from singer_sdk.sinks import SQLSink

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    from collections.abc import Iterable


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

    @override
    def get_sqlalchemy_url(self, config: dict) -> str:
        """Generates a SQLAlchemy URL for {{ cookiecutter.destination_name }}.

        Args:
            config: The configuration for the connector.
        """
        return super().get_sqlalchemy_url(config)


class {{ cookiecutter.destination_name }}Sink(SQLSink):
    """{{ cookiecutter.destination_name }} target sink class."""

    connector_class = {{ cookiecutter.destination_name }}Connector

    def setup(self) -> None:
        """Set up Sink.

        Creates the required Schema and Table entities in the target database.
        """
        super().setup()

    @override
    def process_batch(self, context: dict) -> None:
        """Process a batch with the given batch context.

        Writes a batch to the SQL target. Developers may override this method
        in order to provide a more efficient upload/upsert process.
        """
        super().process_batch(context)

    @override
    def bulk_insert_records(
        self,
        full_table_name: str | FullyQualifiedName,
        schema: dict,
        records: Iterable[dict[str, Any]],
    ) -> int | None:
        """Bulk insert records to an existing destination table.

        The default implementation uses a generic SQLAlchemy bulk insert operation.
        This method may optionally be overridden by developers in order to provide
        faster, native bulk uploads.
        """
        return super().bulk_insert_records(
            full_table_name=full_table_name,
            schema=schema,
            records=records,
        )
