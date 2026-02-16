"""Test class creation."""

from __future__ import annotations

import shutil
import uuid
from pathlib import Path

import pytest
from target_parquet.target import TargetParquet

from singer_sdk.testing import get_target_test_class

StandardTests = get_target_test_class(
    target_class=TargetParquet,
    config=None,  # Config will be provided via plugin_config fixture
)


class TestSampleTargetParquet(StandardTests):
    """Standard Target Tests."""

    @pytest.fixture(scope="class")
    def test_output_dir(self):
        """Create a unique output directory for this test class."""
        output_dir = Path(f".output/test_{uuid.uuid4()}/")
        output_dir.mkdir(parents=True, exist_ok=True)
        yield output_dir
        shutil.rmtree(output_dir)

    @pytest.fixture
    def plugin_config(self, test_output_dir: Path) -> dict:
        """Generate target config using the test output directory fixture.

        This demonstrates composing plugin_config from another fixture
        (test_output_dir) for dynamic file path creation.
        """
        filepath = test_output_dir / "testfile.parquet"
        return {"filepath": str(filepath)}

    @pytest.fixture(scope="class")
    def resource(self, test_output_dir):
        """Provide the output directory as a resource."""
        return test_output_dir
