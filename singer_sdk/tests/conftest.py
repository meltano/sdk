import os
import shutil

import pytest


@pytest.fixture(scope="class")
def outdir() -> str:
    name = ".output/"
    os.mkdir(name)
    yield name
    shutil.rmtree(name)
