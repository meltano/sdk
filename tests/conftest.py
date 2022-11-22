"""Top level test fixtures."""

from __future__ import annotations

import os
import pathlib
import platform
import shutil

import pytest
from _pytest.config import Config

SYSTEMS = {"linux", "darwin", "windows"}


def pytest_collection_modifyitems(config: Config, items: list[pytest.Item]):
    rootdir = pathlib.Path(config.rootdir)

    for item in items:
        rel_path = pathlib.Path(item.fspath).relative_to(rootdir)

        # Mark all tests under tests/external*/ as 'external'
        if rel_path.parts[1].startswith("external"):
            item.add_marker("external")


def pytest_runtest_setup(item):
    supported_systems = SYSTEMS.intersection(mark.name for mark in item.iter_markers())
    system = platform.system().lower()
    if supported_systems and system not in supported_systems:
        pytest.skip(f"cannot run on platform {system}")


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


@pytest.fixture(scope="session")
def snapshot_dir() -> pathlib.Path:
    """Return the path to the snapshot directory."""
    return pathlib.Path("tests/snapshots/")
