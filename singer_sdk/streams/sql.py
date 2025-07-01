"""Base class for SQL-type streams."""

from __future__ import annotations

import abc
import typing as t
from functools import cached_property

import sqlalchemy as sa

import singer_sdk.helpers._catalog as catalog
from singer_sdk.connectors import SQLConnector
from singer_sdk.singerlib import CatalogEntry, MetadataMapping
from singer_sdk.streams.core import REPLICATION_INCREMENTAL, Stream

if t.TYPE_CHECKING:
    from singer_sdk.connectors.sql import FullyQualifiedName
    from singer_sdk.helpers.types import Context, Record
    from singer_sdk.tap_base import Tap


class SQLStream(Stream, metaclass=abc.ABCMeta):
    """Base class for SQLAlchemy-based streams."""

    connector_class = SQLConnector
    _cached_schema: dict | None = None

    supports_nulls_first: bool = False
    """Whether the database supports the NULLS FIRST/LAST syntax."""

    def __init__(
        self,
        tap: Tap,
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
        return CatalogEntry.from_dict(self.catalog_entry)

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

    @cached_property
    def schema(self) -> dict:
        """Return metadata object (dict) as specified in the Singer spec.

        Metadata from an input catalog will override standard metadata.

        Returns:
            The schema object.
        """
        return self._singer_catalog_entry.schema.to_dict()

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
    def primary_keys(self) -> t.Sequence[str]:
        """Get primary keys from the catalog entry definition.

        Returns:
            A list of primary key(s) for the stream.
        """
        return self._singer_catalog_entry.metadata.root.table_key_properties or []

    @primary_keys.setter
    def primary_keys(self, new_value: t.Sequence[str]) -> None:
        """Set or reset the primary key(s) in the stream's catalog entry.

        Args:
            new_value: a list of one or more column names
        """
        self._singer_catalog_entry.metadata.root.table_key_properties = new_value

    @property
    def fully_qualified_name(self) -> FullyQualifiedName:
        """Generate the fully qualified version of the table name.

        Raises:
            ValueError: If table_name is not able to be detected.

        Returns:
            The fully qualified name.
        """
        catalog_entry = self._singer_catalog_entry
        if not catalog_entry.table:
            msg = f"Missing table name in catalog entry: {catalog_entry.to_dict()}"
            raise ValueError(msg)

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
        )

    @property
    def effective_schema(self) -> dict:
        """Return the effective schema for the stream.

        Returns:
            The effective schema.
        """
        return super().effective_schema

    def apply_query_filters(
        self,
        query: sa.sql.Select,
        table: sa.Table,
        *,
        context: Context | None = None,
    ) -> sa.sql.Select:
        """Apply WHERE and ORDER BY clauses to the query.

        By default, this method applies a replication filter to the query
        and orders the results by the replication key, if a replication key is set.

        Args:
            query: The SQLAlchemy Select object.
            table: The SQLAlchemy Table object.
            context: The context object.

        Returns:
            A SQLAlchemy Select object.
        """
        if self.replication_key:
            column = table.columns[self.replication_key]
            order_by = (
                sa.nulls_first(column.asc())
                if self.supports_nulls_first
                else column.asc()
            )
            query = query.order_by(order_by)

            start_val = self.get_starting_replication_key_value(context)
            if start_val is not None:
                query = query.where(column >= start_val)

        return query

    def apply_query_limit(self, query: sa.sql.Select) -> sa.sql.Select:
        """Apply LIMIT clause to the query.

        By default, this method applies a limit filter to the query
        if the stream has an ABORT_AT_RECORD_COUNT value set.

        The ABORT_AT_RECORD_COUNT limit is incremented by 1 to ensure that the
        `MaxRecordsLimitException` exception is properly raised by caller
        `Stream._sync_records()` if more records are available than can be
        processed.

        Args:
            query: The SQLAlchemy Select object.

        Returns:
            A SQLAlchemy Select object.
        """
        if self.ABORT_AT_RECORD_COUNT is not None:
            # Limit record count to one greater than the abort threshold. This ensures
            # `MaxRecordsLimitException` exception is properly raised by caller
            # `Stream._sync_records()` if more records are available than can be
            # processed.
            query = query.limit(self.ABORT_AT_RECORD_COUNT + 1)

        return query

    def build_query(self, *, context: Context | None = None) -> sa.sql.Select:
        """Build a SQLAlchemy Select object for the stream.

        - Apply WHERE and ORDER BY clauses to the query.
        - Apply a LIMIT clause to the query.

        Args:
            context: The context object.

        Returns:
            A SQLAlchemy Select object.
        """
        selected_column_names = self.get_selected_schema()["properties"].keys()
        table = self.connector.get_table(
            full_table_name=self.fully_qualified_name,
            column_names=selected_column_names,
        )
        query = table.select()
        query = self.apply_query_filters(query, table, context=context)
        return self.apply_query_limit(query)

    # Get records from stream
    def get_records(self, context: Context | None) -> t.Iterable[Record]:
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
        if context:  # pragma: no cover
            msg = f"Stream '{self.name}' does not support partitioning."
            raise NotImplementedError(msg)

        with self.connector._connect() as conn:  # noqa: SLF001
            for row in conn.execute(self.build_query(context=context)).mappings():
                # https://github.com/sqlalchemy/sqlalchemy/discussions/10053#discussioncomment-6344965
                yield dict(row)

    @property
    def is_sorted(self) -> bool:
        """Expect stream to be sorted.

        When `True`, incremental streams will attempt to resume if unexpectedly
        interrupted.

        Returns:
            `True` if stream is sorted. Defaults to `False`.
        """
        return self.replication_method == REPLICATION_INCREMENTAL


__all__ = ["SQLConnector", "SQLStream"]
