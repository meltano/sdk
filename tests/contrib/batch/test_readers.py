from __future__ import annotations

import decimal
import gzip
import io
import typing as t

import pytest

from singer_sdk.batch import BaseBatchReader
from singer_sdk.contrib.batch_decoder_arrow import ArrowReader
from singer_sdk.contrib.batch_decoder_jsonl import JSONLinesReader
from singer_sdk.contrib.batch_decoder_parquet import ParquetReader
from singer_sdk.exceptions import MissingDependencyError
from singer_sdk.helpers._batch import BaseBatchFileEncoding
from tests.contrib.batch.helpers import block_import

RECORDS = [
    {"id": 1, "name": "a", "nested": {"x": 1}},
    {"id": 2, "name": None, "nested": {"x": 2}},
]


@pytest.mark.parametrize(
    "fmt, reader_class",
    [
        ("jsonl", JSONLinesReader),
        ("parquet", ParquetReader),
        ("arrow", ArrowReader),
    ],
)
def test_from_encoding(fmt: str, reader_class: type[BaseBatchReader]) -> None:
    reader = BaseBatchReader.from_encoding(BaseBatchFileEncoding(format=fmt))
    assert isinstance(reader, reader_class)


def test_unsupported_format() -> None:
    with pytest.raises(NotImplementedError, match="Unsupported batch encoding"):
        BaseBatchReader.from_encoding(BaseBatchFileEncoding(format="csv"))


@pytest.mark.parametrize("compression", [None, "gzip"])
def test_jsonl_reader(compression: str | None) -> None:
    payload = b'{"id": 1}\n{"id": 2}\n'
    if compression:
        payload = gzip.compress(payload)
    reader = JSONLinesReader(BaseBatchFileEncoding("jsonl", compression))
    assert list(reader.read(io.BytesIO(payload))) == [{"id": 1}, {"id": 2}]


def test_parquet_reader() -> None:
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    buf = io.BytesIO()
    pq.write_table(pa.Table.from_pylist(RECORDS), buf)
    buf.seek(0)
    reader = ParquetReader(BaseBatchFileEncoding("parquet"))
    assert list(reader.read(t.cast("t.IO[bytes]", buf))) == RECORDS


def test_arrow_reader() -> None:
    pa = pytest.importorskip("pyarrow")
    pytest.importorskip("nanoarrow")
    buf = io.BytesIO()
    table = pa.Table.from_pylist(RECORDS)
    with pa.ipc.new_file(buf, table.schema) as writer:
        writer.write_table(table)
    buf.seek(0)
    reader = ArrowReader(BaseBatchFileEncoding("arrow"))
    assert list(reader.read(buf)) == RECORDS


def test_arrow_reader_decodes_json(recwarn: pytest.WarningsRecorder) -> None:
    """``arrow.json`` values are decoded at the top level, in structs and in lists."""
    pa = pytest.importorskip("pyarrow")
    pytest.importorskip("nanoarrow")

    def json_(*values: str | None) -> t.Any:
        return pa.array(list(values), type=pa.json_())

    table = pa.table(
        {
            "id": [1, 2, 3],
            "payload": json_('{"a": [1, 2.5]}', None, "null"),
            "document": pa.StructArray.from_arrays(
                [pa.array(["Ada", "Bo", None]), json_('{"city": "Oslo"}', '"x"', None)],
                names=["name", "address"],
                mask=pa.array([False, False, True]),
            ),
            "tags": pa.ListArray.from_arrays(
                pa.array([0, 2, 3, 3], pa.int32()),
                json_("1", '{"k": null}', "[]"),
                mask=pa.array([False, False, True]),
            ),
            "counts": pa.array([[1], [], None], pa.list_(pa.int64())),
        },
    )
    buf = io.BytesIO()
    with pa.ipc.new_file(buf, table.schema) as writer:
        writer.write_table(table)
    buf.seek(0)

    reader = ArrowReader(BaseBatchFileEncoding("arrow"))
    assert list(reader.read(buf)) == [
        {
            "id": 1,
            "payload": {"a": [1, decimal.Decimal("2.5")]},
            "document": {"name": "Ada", "address": {"city": "Oslo"}},
            "tags": [1, {"k": None}],
            "counts": [1],
        },
        {
            "id": 2,
            "payload": None,
            "document": {"name": "Bo", "address": "x"},
            "tags": [[]],
            "counts": [],
        },
        {"id": 3, "payload": None, "document": None, "tags": None, "counts": None},
    ]
    assert not [w for w in recwarn if "arrow.json" in str(w.message)]


def test_arrow_reader_rejects_non_ipc_file() -> None:
    pytest.importorskip("nanoarrow")
    reader = ArrowReader(BaseBatchFileEncoding("arrow"))
    with pytest.raises(ValueError, match="Not an Arrow IPC file"):
        reader.read(io.BytesIO(b"not arrow at all"))


@pytest.mark.parametrize(
    "reader_class, package, hint",
    [
        (ArrowReader, "nanoarrow", "pip install 'nanoarrow'"),
        (ParquetReader, "pyarrow", "pip install 'singer-sdk[parquet]'"),
    ],
)
def test_reader_missing_dependency(
    monkeypatch: pytest.MonkeyPatch,
    reader_class: type[BaseBatchReader],
    package: str,
    hint: str,
) -> None:
    block_import(monkeypatch, package)
    reader = reader_class(BaseBatchFileEncoding("x"))

    with pytest.raises(MissingDependencyError, match=f"'{package}' package") as exc:
        reader.read(io.BytesIO(b"ARROW1\x00\x00"))

    assert hint in str(exc.value)
