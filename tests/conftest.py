"""Top level test fixtures."""

from __future__ import annotations

import pathlib
import platform
import shutil
import typing as t

import pytest

if t.TYPE_CHECKING:
    from _pytest.config import Config

SYSTEMS = {"linux", "darwin", "windows"}

pytest_plugins = ("singer_sdk.testing.pytest_plugin",)


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
def outdir() -> t.Generator[str, None, None]:
    """Create a temporary directory for cookiecutters and target output."""
    name = ".output/"
    try:
        pathlib.Path(name).mkdir(parents=True)
    except FileExistsError:
        # Directory already exists
        shutil.rmtree(name)
        pathlib.Path(name).mkdir(parents=True)

    yield name
    shutil.rmtree(name)


@pytest.fixture(scope="session")
def snapshot_dir() -> pathlib.Path:
    """Return the path to the snapshot directory."""
    return pathlib.Path("tests/snapshots/")
