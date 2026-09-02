"""Arrow IPC batch encoder."""

from __future__ import annotations

import sys
import typing as t
from uuid import uuid4

from singer_sdk.batch import BaseBatcher, lazy_chunked_generator

if sys.version_info >= (3, 12):
    from typing import override  # noqa: ICN003
else:
    from typing_extensions import override

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Record

__all__ = ["ArrowBatcher"]


class ArrowBatcher(BaseBatcher):
    """Arrow Record Batcher."""

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
        import pyarrow as pa  # noqa: PLC0415

        sync_id = f"{self.tap_name}--{self.stream_name}-{uuid4()}"
        prefix = self.batch_config.storage.prefix or ""

        for i, chunk in enumerate(
            lazy_chunked_generator(
                records,
                self.batch_config.batch_size,
            ),
            start=1,
        ):
            filename = f"{prefix}{sync_id}={i}.arrow"
            storage = self.batch_config.storage
            table = pa.Table.from_pylist(list(chunk))
            with (
                storage.open(filename, "wb") as f,
                pa.ipc.new_file(f, table.schema) as writer,  # type: ignore[arg-type] # ty: ignore[invalid-argument-type]
            ):
                writer.write_table(table)

            file_url = storage.get_url(filename)
            yield [file_url]
