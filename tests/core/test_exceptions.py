"""Tests for the singer_sdk exception hierarchy."""

from __future__ import annotations

import abc

import pytest

import singer_sdk.exceptions as exc

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def assert_hierarchy(*chain: type) -> None:
    """Assert each type in *chain* is a subclass of all types that precede it."""
    for i, cls in enumerate(chain):
        for ancestor in chain[:i]:
            assert issubclass(cls, ancestor), (
                f"{cls.__name__} should be a subclass of {ancestor.__name__}"
            )


# ---------------------------------------------------------------------------
# Root
# ---------------------------------------------------------------------------


class TestRoot:
    def test_singer_sdk_error_is_exception(self) -> None:
        assert issubclass(exc.SingerSDKError, Exception)


# ---------------------------------------------------------------------------
# Every existing exception is still an Exception and now a SingerSDKError
# ---------------------------------------------------------------------------


EXISTING_EXCEPTIONS = [
    exc.ConfigValidationError,
    exc.DiscoveryError,
    exc.FatalAPIError,
    exc.InvalidReplicationKeyException,
    exc.InvalidStreamSortException,
    exc.MapExpressionError,
    exc.RequestedAbortException,
    exc.MaxRecordsLimitException,
    exc.AbortedSyncExceptionBase,
    exc.AbortedSyncFailedException,
    exc.AbortedSyncPausedException,
    exc.RecordsWithoutSchemaException,
    exc.RetriableAPIError,
    exc.StreamMapConfigError,
    exc.TapStreamConnectionFailure,
    exc.TooManyRecordsException,
    exc.ConformedNameClashException,
    exc.MissingKeyPropertiesError,
    exc.InvalidJSONSchema,
    exc.InvalidRecord,
]


@pytest.mark.parametrize("cls", EXISTING_EXCEPTIONS, ids=lambda c: c.__name__)
def test_existing_still_exception(cls: type) -> None:
    assert issubclass(cls, Exception)


@pytest.mark.parametrize("cls", EXISTING_EXCEPTIONS, ids=lambda c: c.__name__)
def test_existing_now_singer_sdk_error(cls: type) -> None:
    assert issubclass(cls, exc.SingerSDKError)


# ---------------------------------------------------------------------------
# Configuration group
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

    def test_config_validation_error_preserves_init(self) -> None:
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


# ---------------------------------------------------------------------------
# Discovery group
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


# ---------------------------------------------------------------------------
# Mapping group
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
# Sync — base
# ---------------------------------------------------------------------------


class TestSyncBase:
    def test_sync_error_chain(self) -> None:
        assert_hierarchy(Exception, exc.SingerSDKError, exc.SyncError)


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

    def test_retriable_api_error_preserves_init(self) -> None:
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

    def test_ignorable_api_error_is_leaf(self) -> None:
        """IgnorableAPIError should be directly instantiable (no abstract methods)."""
        err = exc.IgnorableAPIError("skipping this one")
        assert str(err) == "skipping this one"

    def test_invalid_record_chain(self) -> None:
        assert_hierarchy(
            Exception,
            exc.SingerSDKError,
            exc.SyncError,
            exc.IgnorableSyncError,
            exc.InvalidRecord,
        )

    def test_invalid_record_preserves_init(self) -> None:
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
# __all__ completeness
# ---------------------------------------------------------------------------


class TestAll:
    def test_all_defined(self) -> None:
        assert hasattr(exc, "__all__")

    def test_new_classes_in_all(self) -> None:
        new_classes = [
            "SingerSDKError",
            "ConfigurationError",
            "MappingError",
            "SyncError",
            "FatalSyncError",
            "RetriableSyncError",
            "IgnorableSyncError",
            "IgnorableAPIError",
            "DataError",
            "SyncLifecycleSignal",
        ]
        for name in new_classes:
            assert name in exc.__all__, f"{name} missing from __all__"

    def test_existing_classes_in_all(self) -> None:
        for cls in EXISTING_EXCEPTIONS:
            assert cls.__name__ in exc.__all__, f"{cls.__name__} missing from __all__"

    def test_invalid_input_line_in_all(self) -> None:
        assert "InvalidInputLine" in exc.__all__
