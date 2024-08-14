"""Defines a common set of exceptions which developers can raise and/or catch."""

from __future__ import annotations

import abc
import typing as t

from singer_sdk._singerlib.exceptions import InvalidInputLine  # noqa: F401

if t.TYPE_CHECKING:
    import requests


class ConfigValidationError(Exception):
    """Raised when a user's config settings fail validation."""

    def __init__(
        self,
        message: str,
        *,
        errors: list[str] | None = None,
    ) -> None:
        """Initialize a ConfigValidationError.

        Args:
            message: A message describing the error.
            errors: A list of errors which caused the validation error.
        """
        super().__init__(message)
        self.errors = errors or []


class FatalAPIError(Exception):
    """Exception raised when a failed request should not be considered retriable."""


class InvalidReplicationKeyException(Exception):
    """Exception to raise if the replication key is not in the stream properties."""


class InvalidStreamSortException(Exception):
    """Exception to raise if sorting errors are found while syncing the records."""


class MapExpressionError(Exception):
    """Failed map expression evaluation."""


class RequestedAbortException(Exception):
    """Base class for abort and interrupt requests.

    Whenever this exception is raised, streams will attempt to shut down gracefully and
    will emit a final resumable `STATE` message if it is possible to do so.
    """


class MaxRecordsLimitException(RequestedAbortException):
    """Exception indicating the sync aborted due to too many records."""


class AbortedSyncExceptionBase(Exception, metaclass=abc.ABCMeta):
    """Base exception to raise when a stream sync is aborted.

    Developers should not raise this directly, and instead should use:
    1. `FatalAbortedSyncException` - Indicates the stream aborted abnormally and was not
       able to reach a stable and resumable state.
    2. `PausedSyncException` - Indicates the stream aborted abnormally and successfully
       reached a 'paused' and resumable state.

    Notes:
    - `FULL_TABLE` sync operations cannot be paused and will always trigger a fatal
      exception if aborted.
    - `INCREMENTAL` and `LOG_BASED` streams are able to be paused only if a number of
      preconditions are met, specifically, `state_partitioning_keys` cannot be
      overridden and the stream must be declared with `is_sorted=True`.
    """


class AbortedSyncFailedException(AbortedSyncExceptionBase):
    """Exception to raise when sync is aborted and unable to reach a stable state.

    This signifies that `FULL_TABLE` streams (if applicable) were successfully
    completed, and any bookmarks from `INCREMENTAL` and `LOG_BASED` streams were
    advanced and finalized successfully.
    """


class AbortedSyncPausedException(AbortedSyncExceptionBase):
    """Exception to raise when an aborted sync operation is paused successfully.

    This exception indicates the stream aborted abnormally and successfully
       reached a 'paused' status, and emitted a resumable state artifact before exiting.

    Streams synced with `FULL_TABLE` replication can never have partial success or
    'paused' status.

    If this exception is raised, this signifies that additional records were left
    on the source system and the sync operation aborted before reaching the end of the
    stream. This exception signifies that bookmarks from `INCREMENTAL`
    and `LOG_BASED` streams were successfully emitted and are resumable.
    """


class RecordsWithoutSchemaException(Exception):
    """Raised if a target receives RECORD messages prior to a SCHEMA message."""


class RetriableAPIError(Exception):
    """Exception raised when a failed request can be safely retried."""

    def __init__(self, message: str, response: requests.Response | None = None) -> None:
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


class MissingKeyPropertiesError(Exception):
    """Raised when a received (and/or transformed) record is missing key properties."""


class InvalidJSONSchema(Exception):
    """Raised when a JSON schema is invalid."""


class InvalidRecord(Exception):
    """Raised when a stream record is invalid according to its declared schema."""

    def __init__(self, error_message: str, record: dict) -> None:
        """Initialize an InvalidRecord exception.

        Args:
            error_message: A message describing the error.
            record: The invalid record.
        """
        super().__init__(f"Record Message Validation Error: {error_message}")
        self.error_message = error_message
        self.record = record
