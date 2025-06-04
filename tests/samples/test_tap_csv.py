from __future__ import annotations

import datetime
import typing as t

import pytest

from samples.sample_tap_csv.sample_tap_csv import SampleTapCSV
from singer_sdk.testing import SuiteConfig, TapTestRunner, get_tap_test_class

if t.TYPE_CHECKING:
    from samples.sample_tap_csv.client import CSVStream

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
                    "context": {"_sdc_path": "./customers.csv"},
                    "replication_key": "_sdc_modified_at",
                    "replication_key_value": FUTURE.isoformat(),
                }
            ]
        },
        "employees": {
            "partitions": [
                {
                    "context": {"_sdc_path": "./employees.csv"},
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
)


class TestCSVOneStreamPerFileIncremental(_TestCSVOneStreamPerFileIncremental):
    @pytest.mark.xfail(
        reason="There are no records because the state is set to the future.",
        strict=True,
    )
    def test_tap_stream_returns_record(
        self,
        config: SuiteConfig,
        resource: t.Any,
        runner: TapTestRunner,
        stream: CSVStream,
    ):
        super().test_tap_stream_returns_record(config, resource, runner, stream)


TestCSVOneStreamPerFileIncrementalIgnoreNoRecords = get_tap_test_class(
    tap_class=SampleTapCSV,
    config={
        "path": "fixtures/csv",
        "read_mode": "one_stream_per_file",
        "delimiter": "\t",
    },
    state=STATE,
    suite_config=SuiteConfig(ignore_no_records=True),
)
