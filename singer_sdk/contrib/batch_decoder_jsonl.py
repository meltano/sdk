"""JSON Lines batch decoder."""

from __future__ import annotations

__lazy_modules__ = {"gzip", "singer_sdk.helpers.types", "singer_sdk.singerlib.json"}

import gzip
import sys
import typing as t

from singer_sdk.batch import BaseBatchReader
from singer_sdk.singerlib.json import deserialize_json

if sys.version_info >= (3, 12):
    from typing import override  # noqa: ICN003
else:
    from typing_extensions import override

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Record

__all__ = ["JSONLinesReader"]

_GZIP_MAGIC = b"\x1f\x8b"


class JSONLinesReader(BaseBatchReader):
    """JSON Lines batch file reader.

    Expects raw JSON records (one per line), not Singer protocol messages.
    """

    @override
    def read(self, file: t.IO[bytes]) -> t.Iterable[Record]:
        data = file.read()
        # Detect gzip from the bytes: the encoding metadata may not reflect how the
        # file was actually written.
        if self.encoding.compression == "gzip" or data[:2] == _GZIP_MAGIC:
            data = gzip.decompress(data)
        return [deserialize_json(line) for line in data.splitlines() if line]
