"""Batching utilities for Singer SDK."""

from __future__ import annotations

import itertools
import typing as t
import warnings
from abc import ABC, abstractmethod

from singer_sdk.helpers._compat import entry_points

if t.TYPE_CHECKING:
    from singer_sdk.helpers._batch import BatchConfig

_T = t.TypeVar("_T")


def __getattr__(name: str) -> t.Any:  # noqa: ANN401 # pragma: no cover
    if name == "JSONLinesBatcher":
        warnings.warn(
            "The class JSONLinesBatcher was moved to singer_sdk.contrib.batch_encoder_jsonl.",  # noqa: E501
            DeprecationWarning,
            stacklevel=2,
        )

        from singer_sdk.contrib.batch_encoder_jsonl import JSONLinesBatcher  # noqa: PLC0415, I001

        return JSONLinesBatcher

    msg = f"module {__name__} has no attribute {name}"
    raise AttributeError(msg)


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


class Batcher(BaseBatcher):
    """Determines batch type and then serializes batches to that format."""

    def get_batches(self, records: t.Iterator[dict]) -> t.Iterator[list[str]]:
        """Manifest of batches.

        Args:
            records: The records to batch.

        Returns:
            A list of file paths (called a manifest).
        """
        encoding_format = self.batch_config.encoding.format
        batcher_type = self.get_batcher(encoding_format)
        batcher = batcher_type(
            self.tap_name,
            self.stream_name,
            self.batch_config,
        )
        return batcher.get_batches(records)

    @classmethod
    def get_batcher(cls, name: str) -> type[BaseBatcher]:
        """Get a batcher by name.

        Args:
            name: The name of the batcher.

        Returns:
            The batcher class.

        Raises:
            ValueError: If the batcher is not found.
        """
        plugins = entry_points(group="singer_sdk.batch_encoders")

        try:
            plugin = next(filter(lambda x: x.name == name, plugins))
        except StopIteration:
            message = f"Unsupported batcher: {name}"
            raise ValueError(message) from None

        return plugin.load()  # type: ignore[no-any-return]
