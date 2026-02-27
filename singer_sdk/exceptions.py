"""Defines a common set of exceptions which developers can raise and/or catch."""

from __future__ import annotations

import abc
import typing as t

from singer_sdk.singerlib.exceptions import InvalidInputLine

if t.TYPE_CHECKING:
    import requests

__all__ = [  # noqa: RUF022
    # Root
    "SingerSDKError",
    # Configuration
    "ConfigurationError",
    "ConfigValidationError",
    "MapperNotInitialized",
    # Discovery
    "DiscoveryError",
    "EmptySchemaTypeError",
    "InvalidReplicationKeyException",
    "SchemaNotFoundError",
    "SchemaNotValidError",
    "UnsupportedOpenAPISpec",
    "UnsupportedSchemaFormatError",
    # Mapping
    "MappingError",
    "ConformedNameClashException",
    "MapExpressionError",
    "StreamMapConfigError",
    # Sync — base
    "SyncError",
    # Sync — fatal
    "FatalSyncError",
    "FatalAPIError",
    "InvalidStreamSortException",
    "MissingKeyPropertiesError",
    "RecordsWithoutSchemaException",
    "TapStreamConnectionFailure",
    "TooManyRecordsException",
    # Sync — retriable
    "RetriableSyncError",
    "RetriableAPIError",
    # Sync — ignorable
    "IgnorableSyncError",
    "IgnorableAPIError",
    "InvalidRecord",
    # Sync — data quality
    "DataError",
    "InvalidJSONSchema",
    # Lifecycle signals
    "SyncLifecycleSignal",
    "AbortedSyncExceptionBase",
    "AbortedSyncFailedException",
    "AbortedSyncPausedException",
    "MaxRecordsLimitException",
    "RequestedAbortException",
    # Re-exports
    "InvalidInputLine",
]


# ---------------------------------------------------------------------------
# Root
# ---------------------------------------------------------------------------


class SingerSDKError(Exception):
    """Root base class for all Meltano Singer SDK exceptions."""


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


class ConfigurationError(SingerSDKError):
    """Base class for configuration-related errors."""


class ConfigValidationError(ConfigurationError):
    """Raised when a user's config settings fail validation."""

    def __init__(
        self,
        message: str,
        *,
        errors: list[str] | None = None,
        schema: dict | None = None,
    ) -> None:
        """Initialize a ConfigValidationError.

        Args:
            message: A message describing the error.
            errors: A list of errors which caused the validation error.
            schema: The JSON schema that was used for validation.
        """
        super().__init__(message)
        self.errors: list[str] = errors or []
        self.schema: dict | None = schema


class MapperNotInitialized(ConfigurationError):
    """Raised when the mapper is not initialized."""

    def __init__(self) -> None:
        """Initialize the exception."""
        super().__init__("Mapper not initialized. Please call setup_mapper() first.")


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------


class DiscoveryError(SingerSDKError):
    """Raised when a schema discovery error occurs."""


class EmptySchemaTypeError(DiscoveryError):
    """Exception for when trying to detect type from empty type_dict."""

    def __init__(self, *args: object) -> None:
        """Initialize the exception."""
        msg = (
            "Could not detect type from empty type_dict. "
            "Did you forget to define a property in the stream schema?"
        )
        super().__init__(msg, *args)


class InvalidReplicationKeyException(DiscoveryError):
    """Exception to raise if the replication key is not in the stream properties."""


class SchemaNotFoundError(DiscoveryError):
    """Raised when a schema is not found."""


class SchemaNotValidError(DiscoveryError):
    """Raised when a schema is not valid."""


class UnsupportedSchemaFormatError(DiscoveryError):
    """Raised when the schema source format is not supported."""


# Backward-compatible alias (previously UnsupportedOpenAPISpec in schema/source.py)
UnsupportedOpenAPISpec = UnsupportedSchemaFormatError


# ---------------------------------------------------------------------------
# Mapping
# ---------------------------------------------------------------------------


class MappingError(SingerSDKError):
    """Base class for stream-map related errors."""


class ConformedNameClashException(MappingError):
    """Raised when name conforming produces clashes.

    e.g. two columns conformed to the same name
    """


class MapExpressionError(MappingError):
    """Failed map expression evaluation."""


class StreamMapConfigError(MappingError):
    """Raised when a stream map has an invalid configuration."""


# ---------------------------------------------------------------------------
# Sync — base
# ---------------------------------------------------------------------------


class SyncError(SingerSDKError):
    """Base class for runtime sync errors."""


# ---------------------------------------------------------------------------
# Sync — fatal
# ---------------------------------------------------------------------------


class FatalSyncError(SyncError):
    """Raised when an error requires the entire sync to be aborted."""


class FatalAPIError(FatalSyncError):
    """Exception raised when a failed request should not be considered retriable."""


class InvalidStreamSortException(FatalSyncError):
    """Exception to raise if sorting errors are found while syncing the records."""


class MissingKeyPropertiesError(FatalSyncError):
    """Raised when a received (and/or transformed) record is missing key properties."""


class RecordsWithoutSchemaException(FatalSyncError):
    """Raised if a target receives RECORD messages prior to a SCHEMA message."""


class TapStreamConnectionFailure(FatalSyncError):
    """Exception to raise when stream connection fails or stream is disconnected."""


class TooManyRecordsException(FatalSyncError):
    """Exception to raise when query returns more records than max_records."""


# ---------------------------------------------------------------------------
# Sync — retriable
# ---------------------------------------------------------------------------


class RetriableSyncError(SyncError):
    """Raised when an error can be resolved by retrying the request with backoff."""


class RetriableAPIError(RetriableSyncError):
    """Exception raised when a failed request can be safely retried."""

    def __init__(self, message: str, response: requests.Response | None = None) -> None:
        """Extends the default with the failed response as an attribute.

        Args:
            message (str): The Error Message
            response (requests.Response): The response object.
        """
        super().__init__(message)
        self.response = response


# ---------------------------------------------------------------------------
# Sync — ignorable
# ---------------------------------------------------------------------------


class IgnorableSyncError(SyncError):
    """Raised when an error can be logged, skipped, and the sync continued."""


class IgnorableAPIError(IgnorableSyncError):
    """Raised when a failed API request can be safely ignored and the sync continued."""


class InvalidRecord(IgnorableSyncError):
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


# ---------------------------------------------------------------------------
# Sync — data quality
# ---------------------------------------------------------------------------


class DataError(SyncError):
    """Raised when a data quality violation is detected during sync."""


class InvalidJSONSchema(DataError):
    """Raised when a JSON schema is invalid."""


# ---------------------------------------------------------------------------
# Lifecycle signals
# ---------------------------------------------------------------------------


class SyncLifecycleSignal(SingerSDKError):
    """Base class for control-flow signals that manage sync lifecycle."""


class RequestedAbortException(SyncLifecycleSignal):
    """Base class for abort and interrupt requests.

    Whenever this exception is raised, streams will attempt to shut down gracefully and
    will emit a final resumable `STATE` message if it is possible to do so.
    """


class MaxRecordsLimitException(RequestedAbortException):
    """Exception indicating the sync aborted due to too many records."""


class AbortedSyncExceptionBase(SyncLifecycleSignal, abc.ABC):
    """Base exception to raise when a stream sync is aborted.

    Developers should not raise this directly, and instead should use:
    1. `AbortedSyncFailedException` - Indicates the stream aborted abnormally and was
       not able to reach a stable and resumable state.
    2. `AbortedSyncPausedException` - Indicates the stream aborted abnormally and
       successfully reached a 'paused' and resumable state.

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
