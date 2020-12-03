"""Test sample sync"""

import pyarrow as pa
import pyarrow.parquet as pq

from tap_base.tests.sample_tap_parquet.tap import SampleTapParquet

SAMPLE_DATA = [
    pa.array([1, 2, 3, 4]),
    pa.array(["foo", "bar", "baz", None]),
    pa.array([True, None, False, True]),
]
SAMPLE_BATCH = pa.record_batch(SAMPLE_DATA, names=["f0", "f1", "f2"])
SAMPLE_FILENAME = "/mnt/c/Files/Source/tap-base/tap_base/tests/testfile.parquet"
SAMPLE_CONFIG = {"filepath": SAMPLE_FILENAME}
SAMPLE_CONFIG_BAD = {"not_valid": SAMPLE_FILENAME}


def test_parquet_sync_one():
    _parquet_write()
    tap = SampleTapParquet(config=SAMPLE_CONFIG)
    tap.sync_one(tap_stream_id="placeholder", allow_discover=True)


def test_parquet_sync_all():
    _parquet_write()
    tap = SampleTapParquet(config=SAMPLE_CONFIG)
    tap.sync_all(allow_discover=True)


def _parquet_write():
    """Create a parquet file and read data from it."""
    assert SAMPLE_BATCH.num_rows == 4
    assert SAMPLE_BATCH.num_columns == 3

    writer = pq.ParquetWriter(SAMPLE_FILENAME, SAMPLE_BATCH.schema)
    for i in range(5):
        table = pa.Table.from_batches([SAMPLE_BATCH])
        writer.write_table(table)
    writer.close()


def test_parquet_read():
    _parquet_write()

    parquet_file = pq.ParquetFile(SAMPLE_FILENAME)
    print(parquet_file.metadata)
    print(parquet_file.schema)
    assert parquet_file.num_row_groups == 5
    batches = []
    for i in range(parquet_file.num_row_groups):
        batches.append(parquet_file.read_row_group(i))
    assert batches[0].equals(pa.Table.from_batches([SAMPLE_BATCH]))

