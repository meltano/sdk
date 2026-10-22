from __future__ import annotations

import builtins
import decimal
import importlib.util
import io
import json
import logging
import re
import sys
import typing as t
from contextlib import redirect_stdout
from unittest import mock

import pytest

from singer_sdk import Stream, Tap
from singer_sdk.batch import Batcher
from singer_sdk.contrib.batch_encoder_arrow import ArrowBatcher, rechunk_tables
from singer_sdk.exceptions import MissingDependencyError
from singer_sdk.helpers._batch import (
    BaseBatchFileEncoding,
    BatchConfig,
    BatchFileFormat,
    StorageTarget,
)
from singer_sdk.helpers._typing import _warn_unmapped_properties
from singer_sdk.helpers.conform import TypeConformanceLevel
from singer_sdk.sinks import BatchSink
from singer_sdk.typing import IntegerType, PropertiesList, Property, StringType
from tests.contrib.batch.helpers import block_import

if sys.version_info >= (3, 12):
    from typing import override  # noqa: ICN003
else:
    from typing_extensions import override

if t.TYPE_CHECKING:
    from pathlib import Path

    from singer_sdk.helpers.types import Context, Record


def is_pyarrow_installed():
    module_spec = importlib.util.find_spec("pyarrow")
    return module_spec is not None


skip_if_no_pyarrow = pytest.mark.skipif(
    not is_pyarrow_installed(),
    reason="requires pyarrow",
)


@skip_if_no_pyarrow
def test_batcher(tmp_path: Path) -> None:
    root = tmp_path.joinpath("batches")
    root.mkdir()
    config = BatchConfig(
        encoding=BaseBatchFileEncoding(format="arrow"),
        storage=StorageTarget(root=str(root)),
        batch_size=2,
    )
    batcher = ArrowBatcher("tap", "stream", config)
    records = [
        {"id": 1, "numeric": "1.0"},
        {"id": 2, "numeric": "2.0"},
        {"id": 3, "numeric": "3.0"},
    ]
    batches = list(batcher.get_batches(records))
    assert len(batches) == 2
    assert batches[0][0].endswith(".arrow")


@skip_if_no_pyarrow
def test_batcher_with_arrow_encoding():
    batcher = Batcher(
        "tap-test",
        "stream-test",
        batch_config=BatchConfig(
            encoding=BaseBatchFileEncoding(format="arrow"),
            storage=StorageTarget("file:///tmp/sdk-batches"),
            batch_size=2,
        ),
    )
    records = [
        {"id": 1, "numeric": decimal.Decimal("1.0")},
        {"id": 2, "numeric": decimal.Decimal("2.0")},
        {"id": 3, "numeric": decimal.Decimal("3.0")},
    ]

    batches = list(batcher.get_batches(records))
    assert len(batches) == 2
    assert all(len(batch) == 1 for batch in batches)
    assert all(
        re.match(r".*tap-test--stream-test-.*\.arrow", filepath)
        for batch in batches
        for filepath in batch
    )


def test_sink_reads_arrow_batch_files(tmp_path: Path) -> None:
    """Round-trip: ArrowBatcher output is readable by a sink using nanoarrow."""
    pytest.importorskip("pyarrow")
    pytest.importorskip("nanoarrow")

    received: list[dict] = []

    class MySink(BatchSink):
        def process_batch(self, context: dict) -> None:
            received.extend(context["records"])

    root = tmp_path.joinpath("batches")
    root.mkdir()
    config = BatchConfig(
        encoding=BaseBatchFileEncoding(format="arrow"),
        storage=StorageTarget(root=f"file://{root}"),
        batch_size=2,
    )
    records = [
        {"id": 1, "name": "a", "nested": {"x": 1}},
        {"id": 2, "name": None, "nested": {"x": 2}},
        {"id": 3, "name": "c", "nested": {"x": 3}},
    ]
    files = [
        f
        for batch in ArrowBatcher("tap", "s", config).get_batches(records)
        for f in batch
    ]

    sink = MySink(
        target=mock.MagicMock(),
        stream_name="s",
        schema={"type": "object", "properties": {}},
        key_properties=None,
    )
    sink.process_batch_files(BaseBatchFileEncoding(format=BatchFileFormat.ARROW), files)

    assert received == records


def test_batcher_missing_pyarrow(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    block_import(monkeypatch, "pyarrow")
    config = BatchConfig(
        encoding=BaseBatchFileEncoding(format="arrow"),
        storage=StorageTarget(root=str(tmp_path)),
    )
    with pytest.raises(MissingDependencyError, match="pip install 'pyarrow'"):
        next(ArrowBatcher("tap", "stream", config).get_batches([{"id": 1}]))


@skip_if_no_pyarrow
def test_rechunk_tables() -> None:
    pa = pytest.importorskip("pyarrow")

    tables = [
        pa.table({"id": [1, 2, 3]}),
        pa.RecordBatch.from_pydict({"id": [4]}),
        pa.table({"id": []}, schema=pa.schema([("id", pa.int64())])),
        pa.table({"id": [5, 6, 7, 8, 9]}),
    ]
    chunks = list(rechunk_tables(tables, 4))
    assert [chunk["id"].to_pylist() for chunk in chunks] == [
        [1, 2, 3, 4],
        [5, 6, 7, 8],
        [9],
    ]


@skip_if_no_pyarrow
@pytest.mark.parametrize("chunk_size", [0, -1])
def test_rechunk_tables_rejects_non_positive_size(chunk_size: int) -> None:
    pa = pytest.importorskip("pyarrow")

    with pytest.raises(ValueError, match="Batch size must be at least 1"):
        next(rechunk_tables([pa.table({"id": [1]})], chunk_size))


@skip_if_no_pyarrow
def test_rechunk_tables_promotes_schemas() -> None:
    pa = pytest.importorskip("pyarrow")

    tables = [
        pa.table({"id": [1], "name": [None]}),
        pa.table({"id": [2], "name": ["b"], "extra": [True]}),
    ]
    (chunk,) = rechunk_tables(tables, 10)
    assert chunk.to_pylist() == [
        {"id": 1, "name": None, "extra": None},
        {"id": 2, "name": "b", "extra": True},
    ]


class ArrowStream(Stream):
    name = "arrow_stream"
    schema = PropertiesList(
        Property("id", IntegerType),
        Property("value", StringType),
        Property("secret", StringType),
    ).to_dict()
    replication_key = "id"
    is_sorted = True
    id_batches: t.ClassVar[list[list[int]]] = [[1, 2], [3, 4], [5]]

    @override
    def get_records(self, context: Context | None) -> t.Iterable[Record]:
        pytest.fail("get_records should not be called in Arrow batch mode")

    @override
    def get_record_batches(self, context: Context | None) -> t.Iterable[t.Any]:
        pa = pytest.importorskip("pyarrow")

        start = self.get_starting_replication_key_value(context) or 0
        for batch in self.id_batches:
            if ids := [i for i in batch if i > start]:
                yield pa.RecordBatch.from_pydict(
                    {
                        "id": ids,
                        "value": [str(i) for i in ids],
                        "secret": ["x"] * len(ids),
                    },
                )


class UnsortedArrowStream(ArrowStream):
    is_sorted = False
    id_batches: t.ClassVar[list[list[int]]] = [[3, 1], [5, 2], [4]]


class FullTableArrowStream(ArrowStream):
    replication_key = None


class RecordsOnlyStream(Stream):
    """A stream that doesn't implement `get_record_batches`."""

    name = "arrow_stream"
    schema = ArrowStream.schema

    @override
    def get_records(self, context: Context | None) -> t.Iterable[Record]:
        for i in range(1, 4):
            yield {"id": i, "value": str(i), "secret": "x"}


class EmptyArrowStream(ArrowStream):
    """Opts into native Arrow without needing pyarrow to build its (empty) data."""

    @override
    def get_record_batches(self, context: Context | None) -> t.Iterable[t.Any]:
        return []


class UndeclaredColumnArrowStream(ArrowStream):
    """Yields an `extra` column that isn't in the stream schema."""

    @override
    def get_record_batches(self, context: Context | None) -> t.Iterable[t.Any]:
        pa = pytest.importorskip("pyarrow")

        for batch in super().get_record_batches(context):
            table = pa.Table.from_batches([batch])
            yield table.append_column("extra", pa.array(["e"] * table.num_rows))


class PartitionedArrowStream(ArrowStream):
    ids_by_shard: t.ClassVar[dict[str, list[int]]] = {"a": [1, 2], "b": [10, 11]}

    @property
    @override
    def partitions(self) -> list[dict]:
        return [{"shard": shard} for shard in self.ids_by_shard]

    @override
    def get_record_batches(self, context: Context | None) -> t.Iterable[t.Any]:
        pa = pytest.importorskip("pyarrow")

        assert context is not None
        start = self.get_starting_replication_key_value(context) or 0
        ids = [i for i in self.ids_by_shard[context["shard"]] if i > start]
        yield pa.table(
            {"id": ids, "value": [str(i) for i in ids], "secret": ["x"] * len(ids)},
        )


class ParentArrowStream(ArrowStream):
    name = "parent"

    @override
    def get_records(self, context: Context | None) -> t.Iterable[Record]:
        for i in (1, 2):
            yield {"id": i, "value": str(i), "secret": "x"}

    @override
    def get_record_batches(self, context: Context | None) -> t.Iterable[t.Any]:
        pytest.fail("Streams with child streams should be batched from get_records")

    @override
    def get_child_context(self, record: Record, context: Context | None) -> Context:
        return {"parent_id": record["id"]}


class ChildStream(Stream):
    name = "child"
    schema = PropertiesList(Property("parent_id", IntegerType)).to_dict()
    parent_stream_type = ParentArrowStream

    @override
    def get_records(self, context: Context | None) -> t.Iterable[Record]:
        assert context is not None
        yield {"parent_id": context["parent_id"]}


def _sync_arrow_tap(
    tmp_path: Path,
    *stream_classes: type[Stream],
    state: dict | None = None,
    deselected: tuple[str, ...] = ("secret",),
    deselected_streams: tuple[str, ...] = (),
) -> list[dict]:
    class ArrowTap(Tap):
        name = "tap-arrow"
        config_jsonschema = PropertiesList().to_dict()

        @override
        def discover_streams(self) -> list[Stream]:
            return [cls(self) for cls in stream_classes or (ArrowStream,)]

    catalog = ArrowTap(config={}, parse_env_config=False).catalog_dict
    for stream in catalog["streams"]:
        for entry in stream["metadata"]:
            if entry["breadcrumb"][-1:] and entry["breadcrumb"][-1] in deselected:
                entry["metadata"]["selected"] = False
            if (
                not entry["breadcrumb"]
                and stream["tap_stream_id"] in deselected_streams
            ):
                entry["metadata"]["selected"] = False

    tap = ArrowTap(
        config={
            "batch_config": {
                "encoding": {"format": "arrow"},
                "storage": {"root": f"file://{tmp_path}"},
                "batch_size": 3,
            },
        },
        catalog=catalog,
        state=state,
        parse_env_config=False,
    )
    buf = io.StringIO()
    with redirect_stdout(buf):
        tap.sync_all()
    return [json.loads(line) for line in buf.getvalue().splitlines()]


def _read_batch_files(messages: list[dict]) -> list[list[dict]]:
    pa = pytest.importorskip("pyarrow")

    tables = []
    for message in messages:
        if message["type"] == "BATCH":
            (url,) = message["manifest"]
            with pa.ipc.open_file(url.removeprefix("file://")) as reader:
                tables.append(reader.read_all().to_pylist())
    return tables


def _final_bookmark(messages: list[dict]) -> t.Any:
    state = next(m for m in reversed(messages) if m["type"] == "STATE")["value"]
    return state["bookmarks"]["arrow_stream"].get("replication_key_value")


@skip_if_no_pyarrow
def test_stream_get_record_batches(tmp_path: Path) -> None:
    messages = _sync_arrow_tap(tmp_path)
    batches = [m for m in messages if m["type"] == "BATCH"]
    assert [m["encoding"]["format"] for m in batches] == ["arrow", "arrow"]
    assert not [m for m in messages if m["type"] == "RECORD"]

    assert _read_batch_files(messages) == [
        [{"id": 1, "value": "1"}, {"id": 2, "value": "2"}, {"id": 3, "value": "3"}],
        [{"id": 4, "value": "4"}, {"id": 5, "value": "5"}],
    ]

    # A STATE message follows each BATCH message, bookmarking the file's last row
    bookmarks = [
        messages[i + 1]["value"]["bookmarks"]["arrow_stream"]["replication_key_value"]
        for i, m in enumerate(messages)
        if m["type"] == "BATCH"
    ]
    assert bookmarks == [3, 5]

    # Resuming from the final state yields no new data
    state = next(m for m in reversed(messages) if m["type"] == "STATE")["value"]
    resumed = _sync_arrow_tap(tmp_path, state=state)
    assert not [m for m in resumed if m["type"] == "BATCH"]


@skip_if_no_pyarrow
def test_stream_get_record_batches_unsorted(tmp_path: Path) -> None:
    """An unsorted stream bookmarks each file's max value, not its last."""
    messages = _sync_arrow_tap(tmp_path, UnsortedArrowStream)
    assert [[row["id"] for row in rows] for rows in _read_batch_files(messages)] == [
        [3, 1, 5],
        [2, 4],
    ]
    assert _final_bookmark(messages) == 5


@skip_if_no_pyarrow
def test_stream_get_record_batches_full_table(tmp_path: Path) -> None:
    """Without a replication key there's no bookmark; all columns are kept."""
    messages = _sync_arrow_tap(tmp_path, FullTableArrowStream, deselected=())
    assert _read_batch_files(messages) == [
        [
            {"id": 1, "value": "1", "secret": "x"},
            {"id": 2, "value": "2", "secret": "x"},
            {"id": 3, "value": "3", "secret": "x"},
        ],
        [
            {"id": 4, "value": "4", "secret": "x"},
            {"id": 5, "value": "5", "secret": "x"},
        ],
    ]
    assert _final_bookmark(messages) is None


@skip_if_no_pyarrow
def test_stream_without_get_record_batches_batches_records(tmp_path: Path) -> None:
    """Streams that don't produce Arrow data are batched from `get_records`."""
    messages = _sync_arrow_tap(tmp_path, RecordsOnlyStream, deselected=())
    assert _read_batch_files(messages) == [
        [
            {"id": 1, "value": "1", "secret": "x"},
            {"id": 2, "value": "2", "secret": "x"},
            {"id": 3, "value": "3", "secret": "x"},
        ],
    ]


def test_stream_get_record_batches_missing_pyarrow(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    # Block `import pyarrow...` statements too, not just `import_batch_dependency`
    block_import(monkeypatch, "pyarrow")
    real_import = builtins.__import__

    def import_(name: str, *args: t.Any, **kwargs: t.Any) -> t.Any:
        if name.partition(".")[0] == "pyarrow":
            msg = "No module named 'pyarrow'"
            raise ModuleNotFoundError(msg, name="pyarrow")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", import_)
    with pytest.raises(MissingDependencyError, match="pip install 'pyarrow'"):
        _sync_arrow_tap(tmp_path, EmptyArrowStream)


@skip_if_no_pyarrow
@pytest.mark.parametrize(
    "deselected_streams,batched_streams",
    [
        pytest.param((), ["child", "child", "parent"], id="selected-parent"),
        pytest.param(("parent",), ["child", "child"], id="deselected-parent"),
    ],
)
def test_stream_with_children_batches_records(
    tmp_path: Path,
    deselected_streams: tuple[str, ...],
    batched_streams: list[str],
) -> None:
    """Only records produce child contexts, and deselected parents emit nothing."""
    messages = _sync_arrow_tap(
        tmp_path,
        ParentArrowStream,
        ChildStream,
        deselected=(),
        deselected_streams=deselected_streams,
    )
    batches = [m["stream"] for m in messages if m["type"] == "BATCH"]
    assert sorted(batches) == batched_streams
    child_rows = [
        rows
        for m, rows in zip(
            [m for m in messages if m["type"] == "BATCH"],
            _read_batch_files(messages),
            strict=True,
        )
        if m["stream"] == "child"
    ]
    assert child_rows == [[{"parent_id": 1}], [{"parent_id": 2}]]


@skip_if_no_pyarrow
def test_stream_get_record_batches_partitions(tmp_path: Path) -> None:
    """Each partition gets its own call, batch files and bookmark."""
    messages = _sync_arrow_tap(tmp_path, PartitionedArrowStream)
    assert [[row["id"] for row in rows] for rows in _read_batch_files(messages)] == [
        [1, 2],
        [10, 11],
    ]

    state = next(m for m in reversed(messages) if m["type"] == "STATE")["value"]
    partitions = state["bookmarks"]["arrow_stream"]["partitions"]
    assert [(p["context"], p["replication_key_value"]) for p in partitions] == [
        ({"shard": "a"}, 2),
        ({"shard": "b"}, 11),
    ]

    # Resuming starts each partition from its own bookmark
    resumed = _sync_arrow_tap(tmp_path, PartitionedArrowStream, state=state)
    assert not [m for m in resumed if m["type"] == "BATCH"]


@skip_if_no_pyarrow
@pytest.mark.parametrize(
    "level,additional_properties,kept",
    [
        pytest.param(TypeConformanceLevel.RECURSIVE, False, False, id="dropped"),
        pytest.param(TypeConformanceLevel.NONE, False, True, id="no-conformance"),
        pytest.param(TypeConformanceLevel.RECURSIVE, True, True, id="additional"),
    ],
)
def test_stream_get_record_batches_undeclared_columns(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    level: TypeConformanceLevel,
    additional_properties: bool,
    kept: bool,
) -> None:
    """Columns missing from the schema are handled like RECORD mode handles them."""
    _warn_unmapped_properties.cache_clear()
    stream_class = type(
        "Stream",
        (UndeclaredColumnArrowStream,),
        {
            "TYPE_CONFORMANCE_LEVEL": level,
            "schema": {
                **UndeclaredColumnArrowStream.schema,
                "additionalProperties": additional_properties,
            },
        },
    )
    with caplog.at_level(logging.WARNING):
        messages = _sync_arrow_tap(tmp_path, stream_class)

    rows = [row for rows in _read_batch_files(messages) for row in rows]
    assert rows
    assert all(("extra" in row) is kept for row in rows)
    assert ("not found in catalog schema" in caplog.text) is not kept
