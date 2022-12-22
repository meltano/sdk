import logging

import pytest
from click import Command
from click.testing import CliRunner


def test_captured_warnings(cli: Command, caplog: pytest.LogCaptureFixture) -> None:
    runner = CliRunner()
    with caplog.at_level(logging.INFO):
        result = runner.invoke(cli)

    assert result.exit_code == 0
    assert len(caplog.records) == 2

    info_log = caplog.records[0]
    assert info_log.levelname == "INFO"
    assert info_log.name == "root"
    assert "This is an info message" in info_log.message

    warning_log = caplog.records[1]
    assert warning_log.levelname == "WARNING"
    assert warning_log.name == "py.warnings"
    assert "This is a deprecated function" in warning_log.message


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
def test_ignore_warnings(cli: Command, caplog: pytest.LogCaptureFixture) -> None:
    runner = CliRunner()

    with caplog.at_level(logging.INFO):
        result = runner.invoke(cli)

    assert result.exit_code == 0
    assert len(caplog.records) == 1

    info_log = caplog.records[0]
    assert info_log.levelname == "INFO"
    assert info_log.name == "root"
    assert "This is an info message" in info_log.message


@pytest.mark.filterwarnings("error::DeprecationWarning")
def test_error_warnings(cli: Command, caplog: pytest.LogCaptureFixture) -> None:
    runner = CliRunner()
    result = runner.invoke(cli)
    assert result.exit_code == 1
