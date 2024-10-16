"""Test class creation."""

from __future__ import annotations

import shutil
import sys
import uuid
from pathlib import Path

import pytest

from samples.sample_target_parquet.parquet_target import SampleTargetParquet
from singer_sdk.testing import get_target_test_class

SAMPLE_FILEPATH = Path(f".output/test_{uuid.uuid4()}/")
SAMPLE_FILENAME = SAMPLE_FILEPATH / "testfile.parquet"
SAMPLE_CONFIG = {
    "filepath": str(SAMPLE_FILENAME),
}

StandardTests = get_target_test_class(
    target_class=SampleTargetParquet,
    config=SAMPLE_CONFIG,
)


@pytest.mark.xfail(
    sys.version_info >= (3, 13),
    reason="Parquet not supported on Python 3.13 due to PyArrow incompatibility",
    raises=NameError,
)
class TestSampleTargetParquet(StandardTests):
    """Standard Target Tests."""

    @pytest.fixture(scope="class")
    def test_output_dir(self):
        return SAMPLE_FILEPATH

    @pytest.fixture(scope="class")
    def resource(self, test_output_dir):
        test_output_dir.mkdir(parents=True, exist_ok=True)
        yield test_output_dir
        shutil.rmtree(test_output_dir)
