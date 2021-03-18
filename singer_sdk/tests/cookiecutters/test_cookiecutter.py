"""Test cookiecutter template."""

from pathlib import Path

import black
from flake8.api import legacy as flake8
from cookiecutter.main import cookiecutter
import yaml


COOKIECUTTER_DIR = "cookiecutter/tap-template"
TESTS_INPUT_FILE = "cookiecutter/tap-template/cookiecutter.tests.yml"


def test_cookiecutter_templates():
    """Test sync_all() for countries sample."""
    template_inputs = yaml.safe_load(Path(TESTS_INPUT_FILE).read_text())["tests"]
    style_guide_easy = flake8.get_style_guide(
        ignore=["E302", "E303", "E305", "F401", "W391"]
    )
    style_guide_strict = flake8.get_style_guide(
        ignore=[
            "F401",  # imported but unused
        ]
    )
    for input in template_inputs:
        outdir = ".output"
        cookiecutter(
            template=COOKIECUTTER_DIR,
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
            black.format_file_in_place(
                Path(filepath),
                fast=False,
                mode=black.FileMode(),
                write_back=black.WriteBack.YES,
            )
            report = style_guide_strict.check_files([filepath])
            errors = report.get_statistics("E")
            assert (
                not errors
            ), f"Flake8 found violations in second pass of {filepath}: {errors}"
