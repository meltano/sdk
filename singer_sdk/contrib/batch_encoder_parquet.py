"""Parquet Record Batcher."""

from __future__ import annotations

import typing as t
from uuid import uuid4

from singer_sdk.batch import BaseBatcher, lazy_chunked_generator

__all__ = ["ParquetBatcher"]


class ParquetBatcher(BaseBatcher):
    """Parquet Record Batcher."""

    def get_batches(
        self,
        records: t.Iterator[dict],
    ) -> t.Iterator[list[str]]:
        """Yield manifest of batches.

        Args:
            records: The records to batch.

        Yields:
            A list of file paths (called a manifest).
        """
        import pyarrow as pa  # noqa: PLC0415
        import pyarrow.parquet as pq  # noqa: PLC0415

        sync_id = f"{self.tap_name}--{self.stream_name}-{uuid4()}"
        prefix = self.batch_config.storage.prefix or ""

        for i, chunk in enumerate(
            lazy_chunked_generator(
                records,
                self.batch_config.batch_size,
            ),
            start=1,
        ):
            filename = f"{prefix}{sync_id}={i}.parquet"
            if self.batch_config.encoding.compression == "gzip":
                filename = f"{filename}.gz"
            with self.batch_config.storage.fs() as fs:
                with fs.open(filename, "wb") as f:
                    pylist = list(chunk)
                    table = pa.Table.from_pylist(pylist)
                    if self.batch_config.encoding.compression == "gzip":
                        pq.write_table(table, f, compression="GZIP")
                    else:
                        pq.write_table(table, f)

                file_url = fs.geturl(filename)
            yield [file_url]
