"""Batch helpers."""

from __future__ import annotations

import enum
import platform
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from typing import IO, TYPE_CHECKING, Any, ClassVar, Generator
from urllib.parse import ParseResult, urlencode, urlparse

import fs

from singer_sdk._singerlib.messages import Message, SingerMessageType

if TYPE_CHECKING:
    from fs.base import FS


class BatchFileFormat(str, enum.Enum):
    """Batch file format."""

    JSONL = "jsonl"
    """JSON Lines format."""


@dataclass
class BaseBatchFileEncoding:
    """Base class for batch file encodings."""

    registered_encodings: ClassVar[dict[str, type[BaseBatchFileEncoding]]] = {}
    __encoding_format__: ClassVar[str] = "OVERRIDE_ME"

    # Base encoding fields
    format: str = field(init=False)
    """The format of the batch file."""

    compression: str | None = None
    """The compression of the batch file."""

    def __init_subclass__(cls, **kwargs: Any) -> None:
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
    def from_dict(cls, data: dict[str, Any]) -> BaseBatchFileEncoding:
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
class SDKBatchMessage(Message):
    """Singer batch message in the Meltano Singer SDK flavor."""

    stream: str
    """The stream name."""

    encoding: BaseBatchFileEncoding
    """The file encoding of the batch."""

    manifest: list[str] = field(default_factory=list)
    """The manifest of files in the batch."""

    def __post_init__(self):
        if isinstance(self.encoding, dict):
            self.encoding = BaseBatchFileEncoding.from_dict(self.encoding)

        self.type = SingerMessageType.BATCH


@dataclass
class StorageTarget:
    """Storage target."""

    root: str
    """"The root directory of the storage target."""

    prefix: str | None = None
    """"The file prefix."""

    params: dict = field(default_factory=dict)
    """"The storage parameters."""

    def asdict(self):
        """Return a dictionary representation of the message.

        Returns:
            A dictionary with the defined message fields.
        """
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> StorageTarget:
        """Create an encoding from a dictionary.

        Args:
            data: The dictionary to create the message from.

        Returns:
            The created message.
        """
        return cls(**data)

    @staticmethod
    def split_url(url: str) -> tuple[str, str]:
        """Split a URL into a head and tail pair.

        Args:
            url: The URL to split.

        Returns:
            A tuple of the head and tail parts of the URL.
        """
        if platform.system() == "Windows" and "\\" in url:
            # Original code from pyFileSystem split
            # Augemnted slitly to properly Windows paths
            split = url.rsplit("\\", 1)
            return (split[0] or "\\", split[1])
        else:
            return fs.path.split(url)

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

    @property
    def fs_url(self) -> ParseResult:
        """Get the storage target URL.

        Returns:
            The storage target URL.
        """
        return urlparse(self.root)._replace(query=urlencode(self.params))

    @contextmanager
    def fs(self, **kwargs: Any) -> Generator[FS, None, None]:
        """Get a filesystem object for the storage target.

        Args:
            kwargs: Additional arguments to pass ``f`.open_fs``.

        Returns:
            The filesystem object.
        """
        filesystem = fs.open_fs(self.fs_url.geturl(), **kwargs)
        yield filesystem
        filesystem.close()

    @contextmanager
    def open(self, filename: str, mode: str = "rb") -> Generator[IO, None, None]:
        """Open a file in the storage target.

        Args:
            filename: The filename to open.
            mode: The mode to open the file in.

        Returns:
            The opened file.
        """
        filesystem = fs.open_fs(self.root, writeable=True, create=True)
        fo = filesystem.open(filename, mode=mode)
        try:
            yield fo
        finally:
            fo.close()
            filesystem.close()


@dataclass
class BatchConfig:
    """Batch configuration."""

    encoding: BaseBatchFileEncoding
    """The encoding of the batch file."""

    storage: StorageTarget
    """The storage target of the batch file."""

    def __post_init__(self):
        if isinstance(self.encoding, dict):
            self.encoding = BaseBatchFileEncoding.from_dict(self.encoding)

        if isinstance(self.storage, dict):
            self.storage = StorageTarget.from_dict(self.storage)

    def asdict(self):
        """Return a dictionary representation of the message.

        Returns:
            A dictionary with the defined message fields.
        """
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> BatchConfig:
        """Create an encoding from a dictionary.

        Args:
            data: The dictionary to create the message from.

        Returns:
            The created message.
        """
        return cls(**data)
