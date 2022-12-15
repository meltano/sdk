"""Test class creation."""
import shutil
import sys
import uuid
from pathlib import Path

import pytest

from singer_sdk.testing import get_target_test_class

# temporary exclude of python 3.11
# TODO: remove when pyarrow is supported by 3.11
if sys.version_info < (3, 11):

    from samples.sample_target_parquet.parquet_target import SampleTargetParquet

    SAMPLE_FILEPATH = Path(f".output/test_{uuid.uuid4()}/")
    SAMPLE_FILENAME = SAMPLE_FILEPATH / "testfile.parquet"
    SAMPLE_CONFIG = {"filepath": str(SAMPLE_FILENAME)}

    StandardTests = get_target_test_class(
        target_class=SampleTargetParquet, config=SAMPLE_CONFIG
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
