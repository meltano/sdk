"""JSON Lines batch encoder."""

from __future__ import annotations

import gzip
import typing as t
from uuid import uuid4

from singer_sdk.batch import BaseBatcher, lazy_chunked_generator
from singer_sdk.singerlib.json import serialize_json

__all__ = ["JSONLinesBatcher"]


class JSONLinesBatcher(BaseBatcher):
    """JSON Lines Record Batcher."""

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
        sync_id = f"{self.tap_name}--{self.stream_name}-{uuid4()}"
        storage = self.batch_config.storage
        prefix = storage.prefix or ""

        for i, chunk in enumerate(
            lazy_chunked_generator(
                records,
                self.batch_config.batch_size,
            ),
            start=1,
        ):
            filename = f"{prefix}{sync_id}-{i}.json.gz"
            # TODO: Determine compression from config.
            with (
                storage.open(filename, "wb") as f,
                gzip.GzipFile(fileobj=f, mode="wb") as gz,
            ):
                gz.writelines(
                    (serialize_json(record) + "\n").encode() for record in chunk
                )

            file_url = storage.get_url(filename)
            yield [file_url]
