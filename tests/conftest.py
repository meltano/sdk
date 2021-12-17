"""Top level test fixtures."""

import os
import shutil

import pytest


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
