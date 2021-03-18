"""Test cookiecutter template."""

import yaml
from cookiecutter.main import cookiecutter

from pathlib import Path

COOKIECUTTER_DIR = "cookiecutter/tap-template"
TESTS_INPUT_FILE = "cookiecutter/tap-template/cookiecutter.tests.yml"


def test_cookiecutter_templates():
    """Test sync_all() for countries sample."""
    template_inputs = yaml.safe_load(Path(TESTS_INPUT_FILE).read_text())["tests"]
    for input in template_inputs:
        cookiecutter(
            template=COOKIECUTTER_DIR,
            output_dir=f".output/{input['tap_id']}",
            extra_context=input,
            overwrite_if_exists=True,
            no_input=True,
        )
