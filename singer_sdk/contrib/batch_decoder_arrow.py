"""Arrow IPC batch decoder."""

from __future__ import annotations

__lazy_modules__ = {
    "functools",
    "singer_sdk.helpers.types",
    "singer_sdk.singerlib.json",
    "warnings",
}

import functools
import sys
import typing as t
import warnings

from singer_sdk.batch import BaseBatchReader, import_batch_dependency
from singer_sdk.singerlib.json import deserialize_json

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

# https://arrow.apache.org/docs/format/CanonicalExtensions.html#json
_JSON_EXTENSION = "arrow.json"
# nanoarrow can't convert maps, and its IPC reader rejects list views, so files
# with those types fail before any JSON would be decoded
_LIST_TYPES = frozenset({"LIST", "LARGE_LIST", "FIXED_SIZE_LIST"})


def _decode_json(value: str | None) -> t.Any:  # noqa: ANN401
    return None if value is None else deserialize_json(value)


def _decode_list(
    decode_item: t.Callable[[t.Any], t.Any],
    value: list | None,
) -> list | None:
    return None if value is None else [decode_item(item) for item in value]


def _json_decoder(schema: t.Any) -> t.Callable[[t.Any], t.Any] | None:  # noqa: ANN401
    """Build a function that decodes ``arrow.json`` values in data of this type.

    Args:
        schema: A nanoarrow schema.

    Returns:
        The decoder, or ``None`` if the type contains no ``arrow.json`` values.
    """
    if schema.extension is not None and schema.extension.name == _JSON_EXTENSION:
        return _decode_json

    if schema.type.name == "STRUCT":
        decoders = {
            field.name: decoder
            for field in schema.fields
            if (decoder := _json_decoder(field)) is not None
        }
        if not decoders:
            return None

        def decode_struct(value: dict | None) -> dict | None:
            if value is not None:
                for name, decode in decoders.items():
                    value[name] = decode(value[name])
            return value

        return decode_struct

    if schema.type.name in _LIST_TYPES:
        decode_item = _json_decoder(schema.fields[0])
        if decode_item is None:
            return None
        return functools.partial(_decode_list, decode_item)

    return None


class ArrowReader(BaseBatchReader):
    """Arrow IPC file batch reader. Requires ``nanoarrow``.

    Values of Arrow's canonical JSON extension type (``arrow.json``) are decoded,
    including inside structs and lists, so records match what the same data
    would look like in ``RECORD`` messages.
    """

    @override
    def read(self, file: t.IO[bytes]) -> t.Iterator[Record]:
        """Read records from an Arrow IPC file.

        Args:
            file: The batch file, opened in binary mode.

        Returns:
            An iterator over the records in the file. Records are decoded one
            Arrow chunk at a time, so ``file`` must stay open until the iterator
            is exhausted.

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

        stream = na.ArrayStream.from_readable(file)
        return self._iter_records(stream)

    @staticmethod
    def _iter_records(stream: t.Any) -> t.Iterator[Record]:  # noqa: ANN401
        """Decode the records of an Arrow stream, one chunk at a time.

        The stream is closed once the iterator is exhausted or closed.

        Args:
            stream: A nanoarrow array stream positioned at the start of the data.

        Yields:
            The records in the stream.
        """
        decode = _json_decoder(stream.schema)
        with stream:
            for chunk in stream.iter_chunks():
                with warnings.catch_warnings():
                    # This reader handles arrow.json itself. The filter is scoped to
                    # the conversion so it isn't active while the caller consumes
                    # the records.
                    warnings.filterwarnings(
                        "ignore",
                        message=rf".*unregistered extension '{_JSON_EXTENSION}'",
                    )
                    records = chunk.to_pylist()

                if decode is None:
                    yield from records
                else:
                    for record in records:
                        yield decode(record)
