"""External tests fixtures."""

from __future__ import annotations

import pytest

from singer_sdk.testing import default_vcr_config, use_class_cassette


@pytest.fixture(scope="module")
def vcr_config():
    return default_vcr_config()


@pytest.fixture(scope="class", autouse=True)
def _class_cassette(request: pytest.FixtureRequest, vcr_cassette_dir: str):
    """See `singer_sdk.testing.vcr.use_class_cassette` for why this is needed."""
    yield from use_class_cassette(request, vcr_cassette_dir)
