"""Top level test fixtures."""

from __future__ import annotations

import os
import pathlib
import shutil

import pytest
from _pytest.config import Config


def pytest_collection_modifyitems(config: Config, items: list[pytest.Item]):
    rootdir = pathlib.Path(config.rootdir)

    for item in items:
        rel_path = pathlib.Path(item.fspath).relative_to(rootdir)

        # Mark all tests under tests/external*/ as 'external'
        if rel_path.parts[1].startswith("external"):
            item.add_marker("external")


@pytest.fixture(scope="class")
def outdir() -> str:
    """Create a temporary directory for cookiecutters and target output."""
    name = ".output/"
    try:
        os.mkdir(name)
    except FileExistsError:
        # Directory already exists
        shutil.rmtree(name)
        os.mkdir(name)

    yield name
    shutil.rmtree(name)
