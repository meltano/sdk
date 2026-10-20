"""Parquet batch decoder."""

from __future__ import annotations

__lazy_modules__ = {"singer_sdk.helpers.types"}

import sys
import typing as t

from singer_sdk.batch import BaseBatchReader, import_batch_dependency

if sys.version_info >= (3, 12):
    from typing import override  # noqa: ICN003
else:
    from typing_extensions import override

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Record

__all__ = ["ParquetReader"]


class ParquetReader(BaseBatchReader):
    """Parquet batch file reader. Requires ``pyarrow``."""

    @override
    def read(self, file: t.IO[bytes]) -> t.Iterable[Record]:
        pq = import_batch_dependency(
            "pyarrow.parquet",
            feature="Reading Parquet batch files",
            install="singer-sdk[parquet]",
        )

        return pq.read_table(file).to_pylist()  # type: ignore[no-any-return]
