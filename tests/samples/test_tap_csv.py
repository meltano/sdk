from __future__ import annotations

import datetime

import pytest

from samples.sample_tap_csv.sample_tap_csv import SampleTapCSV
from singer_sdk.testing import SuiteConfig, get_tap_test_class

_TestCSVMerge = get_tap_test_class(
    tap_class=SampleTapCSV,
    config={
        "path": "fixtures/csv",
        "read_mode": "merge",
        "stream_name": "people",
        "delimiter": "\t",
    },
)


class TestCSVMerge(_TestCSVMerge):
    pass


_TestCSVOneStreamPerFile = get_tap_test_class(
    tap_class=SampleTapCSV,
    config={
        "path": "fixtures/csv",
        "read_mode": "one_stream_per_file",
        "delimiter": "\t",
    },
)


class TestCSVOneStreamPerFile(_TestCSVOneStreamPerFile):
    pass


# Three days into the future.
FUTURE = datetime.datetime.now(tz=datetime.timezone.utc) + datetime.timedelta(days=3)

STATE = {
    "bookmarks": {
        "customers": {
            "partitions": [
                {
                    "context": {"_sdc_path": "fixtures/csv/customers.csv"},
                    "replication_key": "_sdc_modified_at",
                    "replication_key_value": FUTURE.isoformat(),
                }
            ]
        },
        "employees": {
            "partitions": [
                {
                    "context": {"_sdc_path": "fixtures/csv/employees.csv"},
                    "replication_key": "_sdc_modified_at",
                    "replication_key_value": FUTURE.isoformat(),
                }
            ]
        },
    }
}


_TestCSVOneStreamPerFileIncremental = get_tap_test_class(
    tap_class=SampleTapCSV,
    config={
        "path": "fixtures/csv",
        "read_mode": "one_stream_per_file",
        "delimiter": "\t",
    },
    state=STATE,
    suite_config=SuiteConfig(ignore_no_records=True),
)


class TestCSVOneStreamPerFileIncremental(_TestCSVOneStreamPerFileIncremental):
    @pytest.mark.xfail(reason="No records are extracted", strict=True)
    def test_tap_stream_transformed_catalog_schema_matches_record(self, stream: str):
        super().test_tap_stream_transformed_catalog_schema_matches_record(stream)

    @pytest.mark.xfail(reason="No records are extracted", strict=True)
    def test_tap_stream_returns_record(self, stream: str):
        super().test_tap_stream_returns_record(stream)
