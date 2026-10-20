"""Batching utilities for Singer SDK."""

from __future__ import annotations

__lazy_modules__ = {
    "enum",
    "importlib",
    "itertools",
    "singer_sdk.exceptions",
    "singer_sdk.helpers._batch",
    "singer_sdk.helpers._compat",
    "singer_sdk.helpers.types",
    "warnings",
}

import importlib
import itertools
import sys
import typing as t
import warnings
from abc import ABC, abstractmethod
from enum import Enum

from singer_sdk.exceptions import MissingDependencyError
from singer_sdk.helpers._compat import entry_points

if sys.version_info >= (3, 12):
    from typing import override  # noqa: ICN003
else:
    from typing_extensions import override

if t.TYPE_CHECKING:
    from singer_sdk.helpers._batch import BaseBatchFileEncoding, BatchConfig
    from singer_sdk.helpers.types import Record

_T = t.TypeVar("_T")


def __getattr__(name: str) -> t.Any:  # noqa: ANN401 # pragma: no cover
    if name == "JSONLinesBatcher":
        warnings.warn(
            "The class JSONLinesBatcher was moved to singer_sdk.contrib.batch_encoder_jsonl.",  # noqa: E501
            DeprecationWarning,
            stacklevel=2,
        )

        from singer_sdk.contrib.batch_encoder_jsonl import JSONLinesBatcher  # ruff: ignore[import-outside-top-level]

        return JSONLinesBatcher

    msg = f"module {__name__} has no attribute {name}"
    raise AttributeError(msg)


def import_batch_dependency(
    module: str,
    *,
    feature: str,
    install: str | None = None,
) -> t.Any:  # noqa: ANN401
    """Import an optional dependency needed to read or write a batch file format.

    Args:
        module: The name of the module to import.
        feature: A description of what needs the module, used in the error message.
        install: The requirement to install, if different from the top-level package.

    Returns:
        The imported module.

    Raises:
        MissingDependencyError: If the module is not installed.
        ModuleNotFoundError: If the module is installed but one of its own
            dependencies is missing.
    """
    try:
        return importlib.import_module(module)
    except ModuleNotFoundError as e:
        package = module.partition(".")[0]
        if e.name != package:
            raise
        raise MissingDependencyError(package, feature=feature, install=install) from e


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
        records: t.Iterable[dict],
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

    @override
    def get_batches(self, records: t.Iterable[dict]) -> t.Iterator[list[str]]:
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


class BaseBatchReader(ABC):
    """Base batch file reader.

    A reader turns the contents of a single batch file into records. It is the
    counterpart of :class:`BaseBatcher`, which writes records to batch files.

    Readers are registered through the ``singer_sdk.batch_decoders`` entry point
    group, keyed by the encoding format name.
    """

    def __init__(self, encoding: BaseBatchFileEncoding) -> None:
        """Initialize the reader.

        Args:
            encoding: The encoding of the batch files to read.
        """
        self.encoding = encoding

    @abstractmethod
    def read(self, file: t.IO[bytes]) -> t.Iterable[Record]:
        """Read records from an open batch file.

        Args:
            file: The batch file, opened in binary mode.

        Raises:
            NotImplementedError: If the method is not implemented.
        """
        raise NotImplementedError

    @classmethod
    def get_reader_class(cls, name: str) -> type[BaseBatchReader]:
        """Get a reader class by encoding format name.

        Args:
            name: The encoding format name, e.g. ``jsonl``.

        Returns:
            The reader class.

        Raises:
            NotImplementedError: If no reader is registered for the format.
        """
        plugins = entry_points(group="singer_sdk.batch_decoders")

        try:
            plugin = next(filter(lambda x: x.name == name, plugins))
        except StopIteration:
            message = f"Unsupported batch encoding format: {name}"
            raise NotImplementedError(message) from None

        return plugin.load()  # type: ignore[no-any-return]

    @classmethod
    def from_encoding(cls, encoding: BaseBatchFileEncoding) -> BaseBatchReader:
        """Create the reader matching a batch file encoding.

        Args:
            encoding: The batch file encoding.

        Returns:
            A reader instance.
        """
        fmt = encoding.format
        name = fmt.value if isinstance(fmt, Enum) else fmt
        return cls.get_reader_class(name)(encoding)
