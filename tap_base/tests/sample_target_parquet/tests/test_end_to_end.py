"""Test tap-to-target sync."""

import io
from pathlib import Path
from contextlib import redirect_stdout
from datetime import datetime

from tap_base.tests.sample_tap_parquet.tap import SampleTapParquet
from tap_base.tests.sample_target_parquet.target import SampleTargetParquet

SAMPLE_FILENAME = "/tmp/testfile.parquet"
SAMPLE_TAP_CONFIG = {"filepath": SAMPLE_FILENAME}
SAMPLE_TARGET_CONFIG = {
    "filepath": f"{SAMPLE_FILENAME.replace('.parquet', '-' + datetime.now().strftime('%Y%m%d-%H%M%S') + '.parquet')}"
}


def test_tap_to_target():
    tap = SampleTapParquet(config=SAMPLE_TAP_CONFIG, state=None)
    with io.StringIO() as buf, redirect_stdout(buf):
        tap.sync_all()
        target = SampleTargetParquet(config=SAMPLE_TARGET_CONFIG)
        target.process_lines(buf)
    # assert Path(SAMPLE_TARGET_CONFIG["filepath"]).exists()
