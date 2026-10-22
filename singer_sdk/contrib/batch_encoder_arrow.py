"""Arrow IPC batch encoder."""

from __future__ import annotations

__lazy_modules__ = {
    "pyarrow",
    "singer_sdk.helpers._batch",
    "singer_sdk.helpers.types",
    "uuid",
}

import sys
import typing as t
from uuid import uuid4

from singer_sdk.batch import (
    BaseBatcher,
    import_batch_dependency,
    lazy_chunked_generator,
)

if sys.version_info >= (3, 12):
    from typing import override  # noqa: ICN003
else:
    from typing_extensions import override

if t.TYPE_CHECKING:
    import pyarrow as pa

    from singer_sdk.helpers._batch import BatchConfig
    from singer_sdk.helpers.types import Record

__all__ = ["ArrowBatcher", "rechunk_tables"]


def _import_pyarrow() -> t.Any:  # noqa: ANN401
    return import_batch_dependency("pyarrow", feature="Writing Arrow batch files")


def rechunk_tables(
    tables: t.Iterable[pa.Table | pa.RecordBatch],
    chunk_size: int,
) -> t.Iterator[pa.Table]:
    """Re-slice a stream of Arrow tables into tables of ``chunk_size`` rows.

    Native Arrow sources (database drivers, ``pymongoarrow``, etc.) produce chunks
    whose sizes don't line up with the configured batch size, so incoming data is
    sliced and buffered until enough rows have accumulated. Buffered chunks are
    concatenated with schema promotion, so chunks whose schemas differ only by
    missing or all-null columns are merged into one table.

    Args:
        tables: Arrow tables or record batches, in order.
        chunk_size: The number of rows per output table. The last table may be
            smaller.

    Yields:
        Arrow tables of at most ``chunk_size`` rows.

    Raises:
        ValueError: If ``chunk_size`` is less than 1.

    .. versionadded:: 0.55.0
    """
    if chunk_size < 1:
        msg = f"Batch size must be at least 1, got {chunk_size}"
        raise ValueError(msg)

    pa = _import_pyarrow()

    buffered: list[t.Any] = []
    buffered_rows = 0

    for item in tables:
        table = (
            pa.Table.from_batches([item]) if isinstance(item, pa.RecordBatch) else item
        )
        offset = 0
        while offset < table.num_rows:
            chunk = table.slice(offset, chunk_size - buffered_rows)
            buffered.append(chunk)
            buffered_rows += chunk.num_rows
            offset += chunk.num_rows

            if buffered_rows >= chunk_size:
                yield pa.concat_tables(buffered, promote_options="default")
                buffered = []
                buffered_rows = 0

    if buffered_rows:
        yield pa.concat_tables(buffered, promote_options="default")


class ArrowBatcher(BaseBatcher):
    """Arrow Record Batcher."""

    def __init__(
        self,
        tap_name: str,
        stream_name: str,
        batch_config: BatchConfig,
    ) -> None:
        """Initialize the batcher.

        Args:
            tap_name: The name of the tap.
            stream_name: The name of the stream.
            batch_config: The batch configuration.
        """
        super().__init__(tap_name, stream_name, batch_config)
        self._sync_id = f"{tap_name}--{stream_name}-{uuid4()}"
        self._file_index = 0

    def write_table(self, table: pa.Table) -> list[str]:
        """Write an Arrow table to a single Arrow IPC file.

        Args:
            table: The table to write.

        Returns:
            A list of file paths (called a manifest).

        .. versionadded:: 0.55.0
        """
        pa = _import_pyarrow()

        self._file_index += 1
        prefix = self.batch_config.storage.prefix or ""
        filename = f"{prefix}{self._sync_id}={self._file_index}.arrow"
        storage = self.batch_config.storage
        with (
            storage.open(filename, "wb") as f,
            pa.ipc.new_file(f, table.schema) as writer,
        ):
            writer.write_table(table)

        return [storage.get_url(filename)]

    @override
    def get_batches(
        self,
        records: t.Iterable[Record],
    ) -> t.Iterator[list[str]]:
        """Yield manifest of batches.

        Args:
            records: The records to batch.

        Yields:
            A list of file paths (called a manifest).
        """
        pa = _import_pyarrow()

        for chunk in lazy_chunked_generator(records, self.batch_config.batch_size):
            yield self.write_table(pa.Table.from_pylist(list(chunk)))
