"""Stream class for reading from files."""

from __future__ import annotations

import abc
import functools
import typing as t

from singer_sdk import Stream
from singer_sdk.exceptions import ConfigValidationError
from singer_sdk.helpers._util import utc_now
from singer_sdk.streams.core import REPLICATION_INCREMENTAL

if t.TYPE_CHECKING:
    import datetime

    import fsspec

    from singer_sdk.helpers.types import Context, Record
    from singer_sdk.tap_base import Tap

SDC_META_FILEPATH = "_sdc_path"
SDC_META_MODIFIED_AT = "_sdc_modified_at"


class FileStream(Stream, metaclass=abc.ABCMeta):
    """Abstract base class for file streams."""

    SDC_PROPERTIES: t.ClassVar[dict[str, dict]] = {
        SDC_META_FILEPATH: {"type": "string"},
        SDC_META_MODIFIED_AT: {"type": ["string", "null"], "format": "date-time"},
    }

    def __init__(
        self,
        tap: Tap,
        name: str,
        *,
        filepaths: t.Sequence[str],
        filesystem: fsspec.AbstractFileSystem,
    ) -> None:
        """Create a new FileStream instance.

        Args:
            tap: The tap for this stream.
            name: The name of the stream.
            filepaths: List of file paths to read.
            filesystem: The filesystem implementation object to use.
            mode: The read mode for the stream.

        Raises:
            ConfigValidationError: If no file paths are provided.
        """
        if not filepaths:  # pragma: no cover
            msg = "Configuration error"
            raise ConfigValidationError(msg, errors=["No file paths provided"])

        self._filepaths = filepaths
        self.filesystem = filesystem

        super().__init__(tap, schema=None, name=name)

        # TODO(edgarrmondragon): Make this None if the filesystem does not support it.
        self.replication_key = SDC_META_MODIFIED_AT
        self._sync_start_time = utc_now()
        self._partitions = [{SDC_META_FILEPATH: path} for path in self._filepaths]

    @property
    def partitions(self) -> list[dict[str, t.Any]]:
        """Return the list of partitions for this stream."""
        return self._partitions

    def _get_full_schema(self) -> dict[str, t.Any]:
        """Return the full schema for the stream.

        Args:
            context: Stream partition or context dictionary.

        Returns:
            The full schema for the stream.
        """
        path: str = self._filepaths[0]
        schema = self.get_schema(path)
        schema["properties"].update(self.SDC_PROPERTIES)
        return schema

    @functools.cached_property
    def schema(self) -> dict[str, t.Any]:
        """Return the schema for the stream."""
        return self._get_full_schema()

    def get_records(
        self,
        context: Context | None,
    ) -> t.Iterable[dict | tuple[dict, dict | None]]:
        """Read records from the file.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            Record or tuple of Record and child context.

        Raises:
            RuntimeError: If context is not provided.
        """
        if not context:  # pragma: no cover
            # TODO: Raise a more specific error.
            msg = f"Context is required for {self.name}"
            raise RuntimeError(msg)

        path: str = context[SDC_META_FILEPATH]

        mtime: datetime.datetime | None
        try:
            mtime: datetime.datetime = self.filesystem.modified(path)  # type: ignore[no-redef]
        except NotImplementedError:  # pragma: no cover
            self.logger.warning("Filesystem does not support modified time")
            mtime = None

        if (
            self.replication_method is REPLICATION_INCREMENTAL
            and (previous_bookmark := self.get_starting_timestamp(context))
            and mtime is not None
            and mtime < previous_bookmark
        ):
            self.logger.info("File has not been modified since last read, skipping")
            return

        for record in self.read_file(path):
            record[SDC_META_MODIFIED_AT] = mtime or self._sync_start_time
            record[SDC_META_FILEPATH] = path
            yield record

    @abc.abstractmethod
    def get_schema(self, path: str) -> dict[str, t.Any]:
        """Return the schema for the file."""

    @abc.abstractmethod
    def read_file(self, path: str) -> t.Iterable[Record]:
        """Return a generator of records from the file."""
