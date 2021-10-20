"""Test target reading from file."""

from pathlib import Path

import pytest

from singer_sdk.samples.sample_target_csv.csv_target import SampleTargetCSV

SAMPLE_FILENAME = Path(__file__).parent / Path("./resources/messages.jsonl")
EXPECTED_OUTPUT = """"id"	"name"
1	"Chris"
2	"Mike"
"""


@pytest.fixture
def target(csv_config: dict):
    return SampleTargetCSV(config=csv_config)


def test_input_arg(target):
    target.listen(SAMPLE_FILENAME)

    output = Path(target.config["target_folder"]) / "./users.csv"
    with open(output, "r") as f:
        assert f.read() == EXPECTED_OUTPUT
