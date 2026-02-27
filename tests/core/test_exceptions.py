"""Tests for the singer_sdk exception hierarchy."""

from __future__ import annotations

import abc

import singer_sdk.exceptions as exc
from singer_sdk.singerlib.exceptions import (
    InvalidInputLine,
    SingerError,
    SingerReadError,
)


def assert_hierarchy(*chain: type) -> None:
    """Assert each type in *chain* is a subclass of all types that precede it."""
    for i, cls in enumerate(chain):
        for ancestor in chain[:i]:
            assert issubclass(cls, ancestor), (
                f"{cls.__name__} should be a subclass of {ancestor.__name__}"
            )


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


class TestConfigurationGroup:
    def test_configuration_error_chain(self) -> None:
        assert_hierarchy(Exception, exc.SingerSDKError, exc.ConfigurationError)

    def test_config_validation_error_chain(self) -> None:
        assert_hierarchy(
            Exception,
            exc.SingerSDKError,
            exc.ConfigurationError,
            exc.ConfigValidationError,
        )

    def test_config_validation_error_init(self) -> None:
        err = exc.ConfigValidationError(
            "bad config", errors=["e1"], schema={"type": "object"}
        )
        assert str(err) == "bad config"
        assert err.errors == ["e1"]
        assert err.schema == {"type": "object"}

    def test_config_validation_error_defaults(self) -> None:
        err = exc.ConfigValidationError("oops")
        assert err.errors == []
        assert err.schema is None

    def test_mapper_not_initialized_chain(self) -> None:
        assert_hierarchy(
            Exception,
            exc.SingerSDKError,
            exc.ConfigurationError,
            exc.MapperNotInitialized,
        )

    def test_mapper_not_initialized_message(self) -> None:
        err = exc.MapperNotInitialized()
        assert "Mapper not initialized" in str(err)
        assert "setup_mapper()" in str(err)


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------


class TestDiscoveryGroup:
    def test_discovery_error_chain(self) -> None:
        assert_hierarchy(Exception, exc.SingerSDKError, exc.DiscoveryError)

    def test_invalid_replication_key_chain(self) -> None:
        assert_hierarchy(
            Exception,
            exc.SingerSDKError,
            exc.DiscoveryError,
            exc.InvalidReplicationKeyException,
        )

    def test_empty_schema_type_error_chain(self) -> None:
        assert_hierarchy(
            Exception,
            exc.SingerSDKError,
            exc.DiscoveryError,
            exc.EmptySchemaTypeError,
        )

    def test_empty_schema_type_error_message(self) -> None:
        err = exc.EmptySchemaTypeError()
        assert "Could not detect type from empty type_dict" in str(err)
        assert "Did you forget to define a property in the stream schema?" in str(err)

    def test_schema_not_found_error_chain(self) -> None:
        assert_hierarchy(
            Exception,
            exc.SingerSDKError,
            exc.DiscoveryError,
            exc.SchemaNotFoundError,
        )

    def test_schema_not_valid_error_chain(self) -> None:
        assert_hierarchy(
            Exception,
            exc.SingerSDKError,
            exc.DiscoveryError,
            exc.SchemaNotValidError,
        )

    def test_unsupported_schema_format_error_chain(self) -> None:
        assert_hierarchy(
            Exception,
            exc.SingerSDKError,
            exc.DiscoveryError,
            exc.UnsupportedSchemaFormatError,
        )

    def test_unsupported_open_api_spec_is_alias(self) -> None:
        assert exc.UnsupportedOpenAPISpec is exc.UnsupportedSchemaFormatError


# ---------------------------------------------------------------------------
# Mapping
# ---------------------------------------------------------------------------


class TestMappingGroup:
    def test_mapping_error_chain(self) -> None:
        assert_hierarchy(Exception, exc.SingerSDKError, exc.MappingError)

    def test_map_expression_error_chain(self) -> None:
        assert_hierarchy(
            Exception, exc.SingerSDKError, exc.MappingError, exc.MapExpressionError
        )

    def test_stream_map_config_error_chain(self) -> None:
        assert_hierarchy(
            Exception, exc.SingerSDKError, exc.MappingError, exc.StreamMapConfigError
        )

    def test_conformed_name_clash_chain(self) -> None:
        assert_hierarchy(
            Exception,
            exc.SingerSDKError,
            exc.MappingError,
            exc.ConformedNameClashException,
        )


# ---------------------------------------------------------------------------
# Sync — fatal
# ---------------------------------------------------------------------------


class TestFatalSync:
    def test_fatal_sync_error_chain(self) -> None:
        assert_hierarchy(
            Exception, exc.SingerSDKError, exc.SyncError, exc.FatalSyncError
        )

    def test_fatal_api_error_chain(self) -> None:
        assert_hierarchy(
            Exception,
            exc.SingerSDKError,
            exc.SyncError,
            exc.FatalSyncError,
            exc.FatalAPIError,
        )

    def test_tap_stream_connection_failure_chain(self) -> None:
        assert_hierarchy(
            Exception,
            exc.SingerSDKError,
            exc.SyncError,
            exc.FatalSyncError,
            exc.TapStreamConnectionFailure,
        )

    def test_too_many_records_chain(self) -> None:
        assert_hierarchy(
            Exception,
            exc.SingerSDKError,
            exc.SyncError,
            exc.FatalSyncError,
            exc.TooManyRecordsException,
        )

    def test_records_without_schema_chain(self) -> None:
        assert_hierarchy(
            Exception,
            exc.SingerSDKError,
            exc.SyncError,
            exc.FatalSyncError,
            exc.RecordsWithoutSchemaException,
        )

    def test_missing_key_properties_chain(self) -> None:
        assert_hierarchy(
            Exception,
            exc.SingerSDKError,
            exc.SyncError,
            exc.FatalSyncError,
            exc.MissingKeyPropertiesError,
        )

    def test_invalid_stream_sort_chain(self) -> None:
        assert_hierarchy(
            Exception,
            exc.SingerSDKError,
            exc.SyncError,
            exc.FatalSyncError,
            exc.InvalidStreamSortException,
        )


# ---------------------------------------------------------------------------
# Sync — retriable
# ---------------------------------------------------------------------------


class TestRetriableSync:
    def test_retriable_sync_error_chain(self) -> None:
        assert_hierarchy(
            Exception, exc.SingerSDKError, exc.SyncError, exc.RetriableSyncError
        )

    def test_retriable_api_error_chain(self) -> None:
        assert_hierarchy(
            Exception,
            exc.SingerSDKError,
            exc.SyncError,
            exc.RetriableSyncError,
            exc.RetriableAPIError,
        )

    def test_retriable_api_error_init(self) -> None:
        err = exc.RetriableAPIError("retry me", response=None)
        assert str(err) == "retry me"
        assert err.response is None


# ---------------------------------------------------------------------------
# Sync — ignorable
# ---------------------------------------------------------------------------


class TestIgnorableSync:
    def test_ignorable_sync_error_chain(self) -> None:
        assert_hierarchy(
            Exception, exc.SingerSDKError, exc.SyncError, exc.IgnorableSyncError
        )

    def test_ignorable_api_error_chain(self) -> None:
        assert_hierarchy(
            Exception,
            exc.SingerSDKError,
            exc.SyncError,
            exc.IgnorableSyncError,
            exc.IgnorableAPIError,
        )

    def test_invalid_record_chain(self) -> None:
        assert_hierarchy(
            Exception,
            exc.SingerSDKError,
            exc.SyncError,
            exc.IgnorableSyncError,
            exc.InvalidRecord,
        )

    def test_invalid_record_init(self) -> None:
        record = {"id": 1}
        err = exc.InvalidRecord("missing field", record)
        assert "missing field" in str(err)
        assert err.error_message == "missing field"
        assert err.record == record


# ---------------------------------------------------------------------------
# Sync — data quality
# ---------------------------------------------------------------------------


class TestDataQuality:
    def test_data_error_chain(self) -> None:
        assert_hierarchy(Exception, exc.SingerSDKError, exc.SyncError, exc.DataError)

    def test_invalid_json_schema_chain(self) -> None:
        assert_hierarchy(
            Exception,
            exc.SingerSDKError,
            exc.SyncError,
            exc.DataError,
            exc.InvalidJSONSchema,
        )


# ---------------------------------------------------------------------------
# Lifecycle signals
# ---------------------------------------------------------------------------


class TestLifecycleSignals:
    def test_sync_lifecycle_signal_chain(self) -> None:
        assert_hierarchy(Exception, exc.SingerSDKError, exc.SyncLifecycleSignal)

    def test_requested_abort_chain(self) -> None:
        assert_hierarchy(
            Exception,
            exc.SingerSDKError,
            exc.SyncLifecycleSignal,
            exc.RequestedAbortException,
        )

    def test_max_records_limit_chain(self) -> None:
        assert_hierarchy(
            Exception,
            exc.SingerSDKError,
            exc.SyncLifecycleSignal,
            exc.RequestedAbortException,
            exc.MaxRecordsLimitException,
        )

    def test_aborted_sync_exception_base_chain(self) -> None:
        assert_hierarchy(
            Exception,
            exc.SingerSDKError,
            exc.SyncLifecycleSignal,
            exc.AbortedSyncExceptionBase,
        )

    def test_aborted_sync_exception_base_is_abstract(self) -> None:
        # abc.ABC is preserved as a base class (signals intent to subclass).
        # No @abstractmethod is declared, so Python does not block instantiation.
        assert issubclass(exc.AbortedSyncExceptionBase, abc.ABC)
        assert type(exc.AbortedSyncExceptionBase) is abc.ABCMeta

    def test_aborted_sync_failed_chain(self) -> None:
        assert_hierarchy(
            Exception,
            exc.SingerSDKError,
            exc.SyncLifecycleSignal,
            exc.AbortedSyncExceptionBase,
            exc.AbortedSyncFailedException,
        )

    def test_aborted_sync_paused_chain(self) -> None:
        assert_hierarchy(
            Exception,
            exc.SingerSDKError,
            exc.SyncLifecycleSignal,
            exc.AbortedSyncExceptionBase,
            exc.AbortedSyncPausedException,
        )


# ---------------------------------------------------------------------------
# Singerlib Singer-protocol hierarchy (intentionally separate from SDK hierarchy)
# ---------------------------------------------------------------------------


class TestSingerlibHierarchy:
    def test_singerlib_hierarchy(self) -> None:
        assert_hierarchy(Exception, SingerError, SingerReadError, InvalidInputLine)

    def test_singerlib_not_in_sdk_hierarchy(self) -> None:
        assert not issubclass(InvalidInputLine, exc.SingerSDKError)
