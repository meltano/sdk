"""Top level test fixtures."""

import os
import shutil

import pytest


@pytest.fixture(scope="class")
def outdir() -> str:
    """Create a temporary directory for cookiecutters and target output."""
    name = ".output/"
    os.mkdir(name)
    yield name
    shutil.rmtree(name)
