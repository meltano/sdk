"""Base class for SQL-type streams."""

from __future__ import annotations

import abc
from typing import Any, Iterable, cast

import sqlalchemy

import singer_sdk.helpers._catalog as catalog
from singer_sdk._singerlib import CatalogEntry, MetadataMapping
from singer_sdk.connectors import SQLConnector
from singer_sdk.plugin_base import PluginBase as TapBaseClass
from singer_sdk.streams.core import Stream


class SQLStream(Stream, metaclass=abc.ABCMeta):
    """Base class for SQLAlchemy-based streams."""

    connector_class = SQLConnector

    def __init__(
        self,
        tap: TapBaseClass,
        catalog_entry: dict,
        connector: SQLConnector | None = None,
    ) -> None:
        """Initialize the database stream.

        If `connector` is omitted, a new connector will be created.

        Args:
            tap: The parent tap object.
            catalog_entry: Catalog entry dict.
            connector: Optional connector to reuse.
        """
        self._connector: SQLConnector
        self._connector = connector or self.connector_class(dict(tap.config))
        self.catalog_entry = catalog_entry
        super().__init__(
            tap=tap,
            schema=self.schema,
            name=self.tap_stream_id,
        )

    @property
    def _singer_catalog_entry(self) -> CatalogEntry:
        """Return catalog entry as specified by the Singer catalog spec.

        Returns:
            A CatalogEntry object.
        """
        return cast(CatalogEntry, CatalogEntry.from_dict(self.catalog_entry))

    @property
    def connector(self) -> SQLConnector:
        """Return a connector object.

        Returns:
            The connector object.
        """
        return self._connector

    @property
    def metadata(self) -> MetadataMapping:
        """Return the Singer metadata.

        Metadata from an input catalog will override standard metadata.

        Returns:
            Metadata object as specified in the Singer spec.
        """
        return self._singer_catalog_entry.metadata

    @property
    def schema(self) -> dict:
        """Return metadata object (dict) as specified in the Singer spec.

        Metadata from an input catalog will override standard metadata.

        Returns:
            The schema object.
        """
        return cast(dict, self._singer_catalog_entry.schema.to_dict())

    @property
    def tap_stream_id(self) -> str:
        """Return the unique ID used by the tap to identify this stream.

        Generally, this is the same value as in `Stream.name`.

        In rare cases, such as for database types with multi-part names,
        this may be slightly different from `Stream.name`.

        Returns:
            The unique tap stream ID as a string.
        """
        return self._singer_catalog_entry.tap_stream_id

    @property
    def primary_keys(self) -> list[str] | None:
        """Get primary keys from the catalog entry definition.

        Returns:
            A list of primary key(s) for the stream.
        """
        return self._singer_catalog_entry.metadata.root.table_key_properties or []

    @primary_keys.setter
    def primary_keys(self, new_value: list[str]) -> None:
        """Set or reset the primary key(s) in the stream's catalog entry.

        Args:
            new_value: a list of one or more column names
        """
        self._singer_catalog_entry.metadata.root.table_key_properties = new_value

    @property
    def fully_qualified_name(self) -> str:
        """Generate the fully qualified version of the table name.

        Raises:
            ValueError: If table_name is not able to be detected.

        Returns:
            The fully qualified name.
        """
        catalog_entry = self._singer_catalog_entry
        if not catalog_entry.table:
            raise ValueError(
                f"Missing table name in catalog entry: {catalog_entry.to_dict()}"
            )

        return self.connector.get_fully_qualified_name(
            table_name=catalog_entry.table,
            schema_name=catalog_entry.metadata.root.schema_name,
            db_name=catalog_entry.database,
        )

    def get_selected_schema(self) -> dict:
        """Return a copy of the Stream JSON schema, dropping any fields not selected.

        Returns:
            A dictionary containing a copy of the Stream JSON schema, filtered
            to any selection criteria.
        """
        return catalog.get_selected_schema(
            stream_name=self.name,
            schema=self.schema,
            mask=self.mask,
            logger=self.logger,
        )

    # Get records from stream
    def get_records(self, context: dict | None) -> Iterable[dict[str, Any]]:
        """Return a generator of record-type dictionary objects.

        If the stream has a replication_key value defined, records will be sorted by the
        incremental key. If the stream also has an available starting bookmark, the
        records will be filtered for values greater than or equal to the bookmark value.

        Args:
            context: If partition context is provided, will read specifically from this
                data slice.

        Yields:
            One dict per record.

        Raises:
            NotImplementedError: If partition is passed in context and the stream does
                not support partitioning.
        """
        if context:
            raise NotImplementedError(
                f"Stream '{self.name}' does not support partitioning."
            )

        selected_column_names = self.get_selected_schema()["properties"].keys()
        table = self.connector.get_table(
            full_table_name=self.fully_qualified_name,
            column_names=selected_column_names,
        )
        query = table.select()

        if self.replication_key:
            replication_key_col = table.columns[self.replication_key]
            query = query.order_by(replication_key_col)

            start_val = self.get_starting_replication_key_value(context)
            if start_val:
                query = query.where(
                    sqlalchemy.text(":replication_key >= :start_val").bindparams(
                        replication_key=replication_key_col, start_val=start_val
                    )
                )

        if self._MAX_RECORDS_LIMIT is not None:
            query = query.limit(self._MAX_RECORDS_LIMIT)

        for record in self.connector.connection.execute(query):
            yield dict(record)


__all__ = ["SQLStream", "SQLConnector"]
