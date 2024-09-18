"""Stream class for reading from files."""

from __future__ import annotations

import abc
import typing as t

from singer_sdk import Stream
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

    BASE_SCHEMA: t.ClassVar[dict[str, t.Any]] = {
        "type": ["object"],
        "properties": {
            SDC_META_FILEPATH: {"type": "string"},
            SDC_META_MODIFIED_AT: {"type": ["string", "null"], "format": "date-time"},
        },
        "required": [],
        "additionalProperties": {"type": "string"},
    }

    def __init__(
        self,
        tap: Tap,
        name: str,
        *,
        filesystem: fsspec.AbstractFileSystem,
        partitions: list[dict[str, t.Any]] | None = None,
    ) -> None:
        """Create a new FileStream instance.

        Args:
            tap: The tap for this stream.
            name: The name of the stream.
            filesystem: The filesystem implementation object to use.
            partitions: List of partitions for this stream.
        """
        # TODO(edgarmondragon): Build schema from file.
        super().__init__(tap, self.BASE_SCHEMA, name)

        # TODO(edgarrmondragon): Make this None if the filesytem does not support it.
        self.replication_key = SDC_META_MODIFIED_AT
        self.filesystem = filesystem
        self._sync_start_time = utc_now()
        self._partitions = partitions or []

    @property
    def partitions(self) -> list[dict[str, t.Any]]:
        """Return the list of partitions for this stream."""
        return self._partitions

    @abc.abstractmethod
    def read_file(self, path: str) -> t.Iterable[Record]:
        """Return a generator of records from the file."""

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
            yield record
