from __future__ import annotations

import datetime
import typing as t

import pytest
from tap_csv.tap import TapCSV

from singer_sdk.testing import SuiteConfig
from singer_sdk.testing.factory import BaseTapTest

if t.TYPE_CHECKING:
    from tap_csv.client import CSVStream

    from singer_sdk.testing import TapTestRunner


class TestCSVMerge(
    BaseTapTest,
    tap_class=TapCSV,
    config={
        "path": "fixtures/csv",
        "read_mode": "merge",
        "stream_name": "people",
        "delimiter": "\t",
    },
): ...


class TestCSVOneStreamPerFile(
    BaseTapTest,
    tap_class=TapCSV,
    config={
        "path": "fixtures/csv",
        "read_mode": "one_stream_per_file",
        "delimiter": "\t",
    },
): ...


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


class TestCSVOneStreamPerFileIncremental(
    BaseTapTest,
    tap_class=TapCSV,
    config={
        "path": "fixtures/csv",
        "read_mode": "one_stream_per_file",
        "delimiter": "\t",
    },
    state=STATE,
):
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


class TestCSVOneStreamPerFileIncrementalIgnoreNoRecords(
    BaseTapTest,
    tap_class=TapCSV,
    config={
        "path": "fixtures/csv",
        "read_mode": "one_stream_per_file",
        "delimiter": "\t",
    },
    state=STATE,
    suite_config=SuiteConfig(ignore_no_records=True),
): ...
