"""Test cookiecutter template."""

import logging
import os

from pathlib import Path

import black
from cookiecutter.main import cookiecutter
from flake8.api import legacy as flake8
from mypy import api
import pytest
import yaml

from logging import getLogger

getLogger("flake8").propagate = False


@pytest.mark.parametrize(
    "cookiecutter_dir",
    [
        "cookiecutter/target-template",
        "cookiecutter/tap-template",
    ],
    ids=["target", "tap"],
)
def test_cookiecutter(outdir: str, cookiecutter_dir: str):
    """Test sync_all() for countries sample."""
    test_input_file = os.path.join(cookiecutter_dir, "cookiecutter.tests.yml")
    template_inputs = yaml.safe_load(Path(test_input_file).read_text())["tests"]
    style_guide_easy = flake8.get_style_guide(
        ignore=["E302", "E303", "E305", "F401", "W391"]
    )
    style_guide_strict = flake8.get_style_guide(
        ignore=[
            "F401",  # "imported but unused"
            "W292",  # "no newline at end of file"
            "W391",  # "blank line at end of file"
        ]
    )
    for input in template_inputs:
        cookiecutter(
            template=cookiecutter_dir,
            output_dir=outdir,
            extra_context=input,
            overwrite_if_exists=True,
            no_input=True,
        )
        for outfile in Path(outdir).glob("**/*.py"):
            filepath = str(outfile.absolute())
            report = style_guide_easy.check_files([filepath])
            errors = report.get_statistics("E")
            assert (
                not errors
            ), f"Flake8 found violations in first pass of {filepath}: {errors}"
            mypy_out = api.run(
                [filepath, "--config", str(Path(outdir) / Path("tox.ini"))]
            )
            mypy_msg = str(mypy_out[0])
            if not mypy_msg.startswith("Success:"):
                logging.exception(f"MyPy validation failed: {mypy_msg}")
                assert not mypy_msg, f"MyPy validation failed for file {filepath}"
            report = style_guide_strict.check_files([filepath])
            errors = report.get_statistics("E")
            assert (
                not errors
            ), f"Flake8 found violations in second pass of {filepath}: {errors}"
            black.format_file_in_place(
                Path(filepath),
                fast=False,
                mode=black.FileMode(),
                write_back=black.WriteBack.NO,
            )
