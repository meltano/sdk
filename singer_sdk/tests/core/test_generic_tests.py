"""Test the generic tests from `singer_sdk.helpers.testing`."""

from singer_sdk.helpers.testing import get_basic_tap_test
from singer_sdk.samples.sample_tap_countries.countries_tap import SampleTapCountries
from singer_sdk.samples.sample_tap_parquet.parquet_tap import SampleTapParquet


PARQUET_TEST_CONFIG = {"filepath": "/tmp/testfile.parquet"}


test_tap_countries_basic_test = get_basic_tap_test(SampleTapCountries)
test_tap_parquet_basic_test = get_basic_tap_test(
    SampleTapParquet, tap_config=PARQUET_TEST_CONFIG
)
