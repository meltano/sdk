"""Defines a common set of exceptions which developers can raise and/or catch."""
import requests


class ConfigValidationError(Exception):
    """Raised when a user's config settings fail validation."""


class FatalAPIError(Exception):
    """Exception raised when a failed request should not be considered retriable."""


class InvalidStreamSortException(Exception):
    """Exception to raise if sorting errors are found while syncing the records."""


class MapExpressionError(Exception):
    """Failed map expression evaluation."""


class MaxRecordsLimitException(Exception):
    """Exception to raise if the maximum number of allowable records is exceeded."""


class RecordsWithoutSchemaException(Exception):
    """Raised if a target receives RECORD messages prior to a SCHEMA message."""


class RetriableAPIError(Exception):
    """Exception raised when a failed request can be safely retried."""

    def __init__(self, message: str, response: requests.Response = None) -> None:
        """Extends the default with the failed response as an attribute.

        Args:
            message (str): The Error Message
            response (requests.Response): The response object.
        """
        super().__init__(message)
        self.response = response


class StreamMapConfigError(Exception):
    """Raised when a stream map has an invalid configuration."""


class TapStreamConnectionFailure(Exception):
    """Exception to raise when stream connection fails or stream is disconnected."""


class TooManyRecordsException(Exception):
    """Exception to raise when query returns more records than max_records."""


class ConformedNameClashException(Exception):
    """Raised when name conforming produces clashes.

    e.g. two columns conformed to the same name
    """
