"""Defines a common set of exceptions which developers can raise and/or catch."""


class TooManyRecordsException(Exception):
    """Exception to raise when query returns more records than max_records."""


class TapStreamConnectionFailure(Exception):
    """Exception to raise when stream connection fails or stream is disconnected."""
