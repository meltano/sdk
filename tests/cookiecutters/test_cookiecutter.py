"""Test cookiecutter template."""

from __future__ import annotations

import logging
from logging import getLogger
from pathlib import Path

import black
import yaml
from cookiecutter.main import cookiecutter
from flake8.api import legacy as flake8
from mypy import api

getLogger("flake8").propagate = False


def pytest_generate_tests(metafunc):
    """Generate test cases for each Cookiecutter template."""
    id_list = []
    argvalues = []

    for template in ["tap", "target"]:
        template_dir = Path(f"cookiecutter/{template}-template")
        case_key = f"{template}_id"
        test_input_file = template_dir.joinpath("cookiecutter.tests.yml")

        for case in yaml.safe_load(test_input_file.read_text())["tests"]:
            id_list.append(case[case_key])
            argvalues.append([template_dir, case])

    metafunc.parametrize(
        ["cookiecutter_dir", "cookiecutter_input"],
        argvalues,
        ids=id_list,
        scope="function",
    )


def test_cookiecutter(outdir: str, cookiecutter_dir: Path, cookiecutter_input: dict):
    """Generate and validate project from Cookiecutter."""
    style_guide_easy = flake8.get_style_guide(
        ignore=["E302", "E303", "E305", "F401", "W391"],
    )
    style_guide_strict = flake8.get_style_guide(
        ignore=[
            "F401",  # "imported but unused"
            "W292",  # "no newline at end of file"
            "W391",  # "blank line at end of file"
        ],
    )
    cookiecutter(
        template=str(cookiecutter_dir),
        output_dir=outdir,
        extra_context=cookiecutter_input,
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
        mypy_out = api.run([filepath, "--config", str(Path(outdir) / Path("tox.ini"))])
        mypy_msg = str(mypy_out[0])
        if not mypy_msg.startswith("Success:"):
            logging.exception("MyPy validation failed: %s", mypy_msg)
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
