"""Test sample sync."""

import pyarrow as pa
import pyarrow.parquet as pq

from singer_sdk.samples.sample_tap_parquet.parquet_tap import SampleTapParquet

COUNTER = 0

SAMPLE_FILENAME = "/tmp/testfile.parquet"
SAMPLE_CONFIG = {"filepath": SAMPLE_FILENAME}
SAMPLE_CONFIG_BAD = {"not_valid": SAMPLE_FILENAME}


def _make_sample_data():
    global COUNTER

    seed = COUNTER * 4
    COUNTER += 1
    return [
        pa.array([seed + 1, seed + 2, seed + 3, seed + 4]),
        pa.array(["foo", "bar", "baz", None]),
        pa.array([True, None, False, True]),
    ]


def _make_sample_batch():
    return pa.record_batch(_make_sample_data(), names=["f0", "f1", "f2"])


def test_parquet_sync_one():
    """Test sync_one() for parquet sample."""
    _parquet_write()
    tap = SampleTapParquet(config=SAMPLE_CONFIG)
    tap.sync_one("ASampleTable")


def test_parquet_sync_all():
    """Test sync_all() for parquet sample."""
    _parquet_write()
    tap = SampleTapParquet(config=SAMPLE_CONFIG)
    tap.sync_all()


def _parquet_write():
    """Create a parquet file and read data from it."""
    sample_batch = _make_sample_batch()
    assert sample_batch.num_rows == 4
    assert sample_batch.num_columns == 3

    writer = pq.ParquetWriter(SAMPLE_FILENAME, sample_batch.schema)

    table = pa.Table.from_batches([sample_batch])
    writer.write_table(table)
    for i in range(5):
        table = pa.Table.from_batches([_make_sample_batch()])
        writer.write_table(table)
    writer.close()


def test_parquet_read():
    """Test generic parquet read functionality."""
    _parquet_write()

    parquet_file = pq.ParquetFile(SAMPLE_FILENAME)
    assert parquet_file.num_row_groups == 5 + 1
    batches = []
    for i in range(parquet_file.num_row_groups):
        batches.append(parquet_file.read_row_group(i))
