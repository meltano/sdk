"""Arrow IPC batch decoder."""

from __future__ import annotations

import sys
import typing as t

from singer_sdk.batch import BaseBatchReader, import_batch_dependency

if sys.version_info >= (3, 12):
    from typing import override  # noqa: ICN003
else:
    from typing_extensions import override

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Record

__all__ = ["ArrowReader"]

# The IPC file format is these magic bytes (padded to 8 bytes) followed by a
# regular IPC stream, which is what nanoarrow can read.
_ARROW_FILE_MAGIC = b"ARROW1\x00\x00"


class ArrowReader(BaseBatchReader):
    """Arrow IPC file batch reader. Requires ``nanoarrow``."""

    @override
    def read(self, file: t.IO[bytes]) -> t.Iterable[Record]:
        """Read records from an Arrow IPC file.

        Args:
            file: The batch file, opened in binary mode.

        Returns:
            The records in the file.

        Raises:
            ValueError: If the file is not in the Arrow IPC file format.
        """
        na = import_batch_dependency(
            "nanoarrow",
            feature="Reading Arrow batch files",
        )

        if file.read(len(_ARROW_FILE_MAGIC)) != _ARROW_FILE_MAGIC:
            msg = "Not an Arrow IPC file"
            raise ValueError(msg)

        return na.ArrayStream.from_readable(file).read_all().to_pylist()  # type: ignore[no-any-return]
