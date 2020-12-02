"""Sample tap test for tap-pardot."""

from typing import List, Union
from pardot.connector.connection import PardotConnection
import pardot.connector

from tap_base import GenericConnectionBase


class TooManyRecordsException(Exception):
    """Exception to raise when query returns more records than max_records."""


class SampleTapPardotConnection(GenericConnectionBase):
    """Pardot Tap Connection Class."""

    _conn: PardotConnection

    def open_connection(self) -> PardotConnection:
        """Connect to pardot database."""
        self._conn = pardot.connector.connect(user=self.get_config("filepath"),)
        return self._conn
