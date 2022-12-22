import logging
import warnings

import click
import pytest

from singer_sdk.cli import SingerCommand


@pytest.fixture
def cli():
    @click.command(cls=SingerCommand)
    def main():
        logging.basicConfig(level=logging.INFO)
        logging.info("This is an info message")
        warnings.warn("This is a deprecated function", DeprecationWarning)

    return main
