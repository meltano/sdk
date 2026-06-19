"""Test class creation."""

from __future__ import annotations

import shutil
import uuid
from pathlib import Path

import pytest
from target_parquet.target import TargetParquet

from singer_sdk.testing import get_target_test_class

SAMPLE_FILEPATH = Path(f".output/test_{uuid.uuid4()}/")
SAMPLE_FILENAME = SAMPLE_FILEPATH / "testfile.parquet"
SAMPLE_CONFIG = {
    "filepath": str(SAMPLE_FILENAME),
}

StandardTests = get_target_test_class(
    target_class=TargetParquet,
    config=SAMPLE_CONFIG,
)


class TestSampleTargetParquet(StandardTests):
    """Standard Target Tests."""

    @pytest.fixture(scope="class")
    @classmethod
    def test_output_dir(cls) -> Path:
        return SAMPLE_FILEPATH

    @pytest.fixture(scope="class")
    @classmethod
    def resource(cls, test_output_dir: Path):
        test_output_dir.mkdir(parents=True, exist_ok=True)
        yield test_output_dir
        shutil.rmtree(test_output_dir)
