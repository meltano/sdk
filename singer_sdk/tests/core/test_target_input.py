"""Test target reading from file."""

import json
import os
from pathlib import Path

import pytest
from click.testing import CliRunner

from singer_sdk.samples.sample_target_csv.csv_target import SampleTargetCSV

SAMPLE_FILENAME = Path(__file__).parent / Path("./resources/messages.jsonl")
EXPECTED_OUTPUT = """"id"	"name"
1	"Chris"
2	"Mike"
"""


@pytest.fixture
def target(csv_config: dict):
    return SampleTargetCSV(config=csv_config)


@pytest.fixture
def cli_runner():
    return CliRunner()


@pytest.fixture
def config_file_path(target):
    try:
        path = Path(target.config["target_folder"]) / "./config.json"
        with open(path, "w") as f:
            f.write(json.dumps(dict(target.config)))
        yield path
    finally:
        os.remove(path)


def test_input_arg(cli_runner, config_file_path, target):
    result = cli_runner.invoke(
        target.cli,
        [
            "--config",
            config_file_path,
            "--input",
            SAMPLE_FILENAME,
        ],
    )

    assert result.exit_code == 0

    output = Path(target.config["target_folder"]) / "./users.csv"
    with open(output, "r") as f:
        assert f.read() == EXPECTED_OUTPUT
