"""Batch helpers."""

from __future__ import annotations

import enum
import typing as t
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from functools import cached_property
from urllib.parse import urlparse

from upath import UPath

from singer_sdk.singerlib.messages import Message, SingerMessageType

if t.TYPE_CHECKING:
    from fsspec import AbstractFileSystem

DEFAULT_BATCH_SIZE = 10000


class BatchFileFormat(str, enum.Enum):
    """Batch file format."""

    JSONL = "jsonl"
    """JSON Lines format."""

    PARQUET = "parquet"
    """Parquet format."""


@dataclass
class BaseBatchFileEncoding:
    """Base class for batch file encodings."""

    registered_encodings: t.ClassVar[dict[str, type[BaseBatchFileEncoding]]] = {}
    __encoding_format__: t.ClassVar[str] = "OVERRIDE_ME"

    # Base encoding fields
    format: str = field(init=False)
    """The format of the batch file."""

    compression: str | None = None
    """The compression of the batch file."""

    def __init_subclass__(cls, **kwargs: t.Any) -> None:
        """Register subclasses.

        Args:
            **kwargs: Keyword arguments.
        """
        super().__init_subclass__(**kwargs)
        cls.registered_encodings[cls.__encoding_format__] = cls

    def __post_init__(self) -> None:
        """Post-init hook."""
        self.format = self.__encoding_format__

    @classmethod
    def from_dict(cls, data: dict[str, t.Any]) -> BaseBatchFileEncoding:
        """Create an encoding from a dictionary."""
        data = data.copy()
        encoding_format = data.pop("format")
        encoding_cls = cls.registered_encodings[encoding_format]
        return encoding_cls(**data)


@dataclass
class JSONLinesEncoding(BaseBatchFileEncoding):
    """JSON Lines encoding for batch files."""

    __encoding_format__ = "jsonl"


@dataclass
class ParquetEncoding(BaseBatchFileEncoding):
    """Parquet encoding for batch files."""

    __encoding_format__ = "parquet"


@dataclass
class SDKBatchMessage(Message):
    """Singer batch message in the Meltano Singer SDK flavor."""

    stream: str
    """The stream name."""

    encoding: BaseBatchFileEncoding
    """The file encoding of the batch."""

    manifest: list[str] = field(default_factory=list)
    """The manifest of files in the batch."""

    def __post_init__(self) -> None:
        if isinstance(self.encoding, dict):
            self.encoding = BaseBatchFileEncoding.from_dict(self.encoding)

        self.type = SingerMessageType.BATCH


@dataclass
class StorageTarget:
    """Storage target for batch files."""

    root: str
    """"The root directory of the storage target."""

    prefix: str | None = None
    """"The file prefix."""

    params: dict = field(default_factory=dict)
    """"The storage parameters."""

    def asdict(self) -> dict[str, t.Any]:
        """Return a dictionary representation of the message.

        Returns:
            A dictionary with the defined message fields.
        """
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, t.Any]) -> StorageTarget:
        """Create an encoding from a dictionary.

        Args:
            data: The dictionary to create the message from.

        Returns:
            The created message.
        """
        return cls(**data)

    @cached_property
    def _root_path(self) -> UPath:
        """The root path of the storage target."""
        return UPath(self.root, **self.params).resolve()

    @staticmethod
    def split_url(url: str) -> tuple[str, str]:
        """Split a URL into a head and tail pair.

        Args:
            url: The URL to split.

        Returns:
            A tuple of the head and tail parts of the URL.
        """
        url_path = UPath(url)
        head, tail = url_path.parts[:-1], url_path.parts[-1]
        return url_path.with_segments(*head).as_uri(), tail

    @classmethod
    def from_url(cls, url: str) -> StorageTarget:
        """Create a storage target from a file URL.

        Args:
            url: The URL to create the storage target from.

        Returns:
            The created storage target.
        """
        parsed_url = urlparse(url)
        new_url = parsed_url._replace(query="")
        return cls(root=new_url.geturl())

    def get_url(self, filename: str) -> str:
        """Get the filename URL for the storage target.

        Args:
            filename: The filename to get the URL for.

        Returns:
            The storage target URL.
        """
        return self._root_path.joinpath(filename).as_uri()

    @contextmanager
    def _fs(self) -> t.Generator[AbstractFileSystem, None, None]:
        """Get filesystem.

        Returns:
            The filesystem object.
        """
        fs = self._root_path.fs
        fs.start_transaction()
        self._root_path.mkdir(parents=True, exist_ok=True)
        yield fs
        fs.end_transaction()

    @contextmanager
    def open(
        self,
        filename: str,
        mode: str = "rb",
    ) -> t.Generator[t.IO, None, None]:
        """Open a file in the storage target.

        Args:
            filename: The filename to open.
            mode: The mode to open the file in.

        Returns:
            The opened file.
        """
        with self._fs() as fs, fs.open(self._root_path / filename, mode) as f:
            yield f


@dataclass
class BatchConfig:
    """Batch configuration."""

    encoding: BaseBatchFileEncoding
    """The encoding of the batch file."""

    storage: StorageTarget
    """The storage target of the batch file."""

    batch_size: int = DEFAULT_BATCH_SIZE
    """The max number of records in a batch."""

    def __post_init__(self) -> None:
        if isinstance(self.encoding, dict):
            self.encoding = BaseBatchFileEncoding.from_dict(self.encoding)

        if isinstance(self.storage, dict):
            self.storage = StorageTarget.from_dict(self.storage)

        if self.batch_size is None:
            self.batch_size = DEFAULT_BATCH_SIZE  # type: ignore[unreachable]

    def asdict(self) -> dict[str, t.Any]:
        """Return a dictionary representation of the message.

        Returns:
            A dictionary with the defined message fields.
        """
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, t.Any]) -> BatchConfig:
        """Create an encoding from a dictionary.

        Args:
            data: The dictionary to create the message from.

        Returns:
            The created message.
        """
        return cls(**data)
