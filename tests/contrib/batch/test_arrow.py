from __future__ import annotations

import decimal
import importlib.util
import io
import json
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


def _sync_arrow_tap(
    tmp_path: Path,
    stream_class: type[Stream] = ArrowStream,
    *,
    state: dict | None = None,
    deselected: tuple[str, ...] = ("secret",),
) -> list[dict]:
    class ArrowTap(Tap):
        name = "tap-arrow"
        config_jsonschema = PropertiesList().to_dict()

        @override
        def discover_streams(self) -> list[Stream]:
            return [stream_class(self)]

    catalog = ArrowTap(config={}, parse_env_config=False).catalog_dict
    for stream in catalog["streams"]:
        for entry in stream["metadata"]:
            if entry["breadcrumb"][-1:] and entry["breadcrumb"][-1] in deselected:
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
