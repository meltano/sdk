# """Test tap-to-target sync."""

# TODO: Rewrite these tests, currently all rely on parquet tap sample (removed)

# import io
# from contextlib import redirect_stdout
# from datetime import datetime

# from singer_sdk.samples.sample_tap_parquet.parquet_tap import SampleTapParquet
# from singer_sdk.samples.sample_target_parquet.parquet_target import SampleTargetParquet
# from singer_sdk.samples.sample_target_csv.csv_target import SampleTargetCSV

# SAMPLE_FILENAME = "/tmp/testfile.parquet"
# SAMPLE_TAP_CONFIG = {"filepath": SAMPLE_FILENAME}
# SAMPLE_TARGET_PARQUET_CONFIG = {
#     "filepath": f"{SAMPLE_FILENAME.replace('.parquet', '-' + datetime.now().strftime('%Y%m%d-%H%M%S') + '.parquet')}"
# }
# SAMPLE_TARGET_CSV_CONFIG = {"target_folder": f"./.output"}


# def sync_end_to_end(tap, target):
#     buf = io.StringIO()
#     with redirect_stdout(buf):
#         tap.sync_all()
#     buf.seek(0)
#     target._process_lines(buf)


# def test_parquet_to_csv():
#     tap = SampleTapParquet(config=SAMPLE_TAP_CONFIG, state=None)
#     target = SampleTargetCSV(config=SAMPLE_TARGET_CSV_CONFIG)
#     sync_end_to_end(tap, target)


# def test_parquet_to_parquet():
#     tap = SampleTapParquet(config=SAMPLE_TAP_CONFIG, state=None)
#     target = SampleTargetParquet(config=SAMPLE_TARGET_PARQUET_CONFIG)
#     sync_end_to_end(tap, target)
#     # assert Path(SAMPLE_TARGET_PARQUET_CONFIG["filepath"]).exists()
