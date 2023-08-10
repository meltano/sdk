"""Batching utilities for Singer SDK."""
from __future__ import annotations

import csv
import gzip
import io
import itertools
import json
import typing as t
from abc import ABC, abstractmethod
from uuid import uuid4

from singer_sdk.exceptions import UnsupportedBatchCompressionError
from singer_sdk.helpers._batch import BatchFileCompression

if t.TYPE_CHECKING:
    from fs.base import FS

    from singer_sdk.helpers._batch import BatchConfig, CSVEncoding

_T = t.TypeVar("_T")


def lazy_chunked_generator(
    iterable: t.Iterable[_T],
    chunk_size: int,
) -> t.Generator[t.Iterator[_T], None, None]:
    """Yield a generator for each chunk of the given iterable.

    Args:
        iterable: The iterable to chunk.
        chunk_size: The size of each chunk.

    Yields:
        A generator for each chunk of the given iterable.
    """
    iterator = iter(iterable)
    while True:
        chunk = list(itertools.islice(iterator, chunk_size))
        if not chunk:
            break
        yield iter(chunk)


class BaseBatcher(ABC):
    """Base Record Batcher."""

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
        self.tap_name = tap_name
        self.stream_name = stream_name
        self.batch_config = batch_config

    @abstractmethod
    def get_batches(
        self,
        records: t.Iterator[dict],
    ) -> t.Iterator[list[str]]:
        """Yield manifest of batches.

        Args:
            records: The records to batch.

        Raises:
            NotImplementedError: If the method is not implemented.
        """
        raise NotImplementedError


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
        prefix = self.batch_config.storage.prefix or ""

        for i, chunk in enumerate(
            lazy_chunked_generator(
                records,
                self.batch_config.batch_size,
            ),
            start=1,
        ):
            filename = f"{prefix}{sync_id}-{i}.json.gz"
            with self.batch_config.storage.fs(create=True) as fs:
                # TODO: Determine compression from config.
                with fs.open(filename, "wb") as f, gzip.GzipFile(
                    fileobj=f,
                    mode="wb",
                ) as gz:
                    gz.writelines(
                        (json.dumps(record, default=str) + "\n").encode()
                        for record in chunk
                    )
                file_url = fs.geturl(filename)
            yield [file_url]


class CSVBatcher(BaseBatcher):
    """JSON array Record Batcher."""

    def __init__(
        self,
        tap_name: str,
        stream_name: str,
        batch_config: BatchConfig[CSVEncoding],
    ) -> None:
        """Initialize the batcher.

        Args:
            tap_name: The name of the tap.
            stream_name: The name of the stream.
            batch_config: The batch configuration.
        """
        super().__init__(tap_name, stream_name, batch_config)
        self.encoding = self.batch_config.encoding

    def get_batches(
        self,
        records: t.Iterator[dict],
    ) -> t.Iterator[list[str]]:
        """Yield manifest of batches.

        Args:
            records: The records to batch.

        Yields:
            A list of file paths (called a manifest).

        Raises:
            UnsupportedBatchCompressionError: If the compression is not supported.
        """
        sync_id = f"{self.tap_name}--{self.stream_name}-{uuid4()}"
        prefix = self.batch_config.storage.prefix or ""

        for i, chunk in enumerate(
            lazy_chunked_generator(
                records,
                self.batch_config.batch_size,
            ),
            start=1,
        ):
            with self.batch_config.storage.fs(create=True) as fs:
                if self.encoding.compression == BatchFileCompression.GZIP:
                    filename = f"{prefix}{sync_id}-{i}.csv.gz"
                    self._write_gzip(fs, filename, chunk)
                elif self.encoding.compression == BatchFileCompression.NONE:
                    filename = f"{prefix}{sync_id}-{i}.csv"
                    self._write_plain(fs, filename, chunk)
                else:
                    raise UnsupportedBatchCompressionError(
                        self.encoding.compression or "none",
                    )
                file_url = fs.geturl(filename)
            yield [file_url]

    def _write_gzip(self, fs: FS, filename: str, records: t.Iterator[dict]) -> None:
        first_record = next(records, None)

        if first_record is None:
            return

        with fs.open(filename, "wb") as f, gzip.GzipFile(
            fileobj=f,
            mode="wb",
        ) as gz:
            string_buffer = io.StringIO()
            writer = csv.DictWriter(
                string_buffer,
                fieldnames=first_record.keys(),
                delimiter=self.encoding.delimiter,
            )
            if self.encoding.header:
                writer.writeheader()
            writer.writerow(first_record)
            writer.writerows(records)
            gz.write(string_buffer.getvalue().encode())

    def _write_plain(self, fs: FS, filename: str, records: t.Iterator[dict]) -> None:
        first_record = next(records, None)

        if first_record is None:
            return

        with fs.open(filename, "w") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=first_record.keys(),
                delimiter=self.encoding.delimiter,
            )
            if self.encoding.header:
                writer.writeheader()
            writer.writeheader()
            writer.writerow(first_record)
            writer.writerows(records)
