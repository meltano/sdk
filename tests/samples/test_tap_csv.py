from __future__ import annotations

import pytest

from samples.sample_tap_csv.sample_tap_csv import SampleTapCSV
from singer_sdk.testing import get_tap_test_class

_TestCSVMerge = get_tap_test_class(
    tap_class=SampleTapCSV,
    config={
        "path": "fixtures/csv",
        "read_mode": "merge",
        "stream_name": "people",
    },
)


class TestCSVMerge(_TestCSVMerge):
    @pytest.mark.xfail(reason="Schema generation not implemented", strict=True)
    def test_tap_stream_record_schema_matches_transformed_catalog(self, stream: str):
        super().test_tap_stream_record_schema_matches_transformed_catalog(stream)


TestCSVOneStreamPerFile = get_tap_test_class(
    tap_class=SampleTapCSV,
    config={
        "path": "fixtures/csv",
        "read_mode": "one_stream_per_file",
    },
)


class TestCSVOneStreamPerFile(TestCSVOneStreamPerFile):
    @pytest.mark.xfail(reason="Schema generation not implemented", strict=True)
    def test_tap_stream_record_schema_matches_transformed_catalog(self, stream: str):
        super().test_tap_stream_record_schema_matches_transformed_catalog(stream)
