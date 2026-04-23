"""Tests for the replication_config class attribute on streams."""

from __future__ import annotations

import sys
import typing as t

import pytest

from singer_sdk import Stream, Tap
from singer_sdk.replication import (
    FullTableReplication,
    IncrementalReplication,
    LogBasedReplication,
)
from singer_sdk.singerlib import Catalog, MetadataMapping
from singer_sdk.singerlib.catalog import (
    REPLICATION_FULL_TABLE,
    REPLICATION_INCREMENTAL,
    REPLICATION_LOG_BASED,
)
from singer_sdk.state_comparators import AscendingComparator, StrictAscendingComparator
from singer_sdk.typing import (
    DateTimeType,
    IntegerType,
    PropertiesList,
    Property,
    StringType,
)

if sys.version_info >= (3, 12):
    from typing import override  # noqa: ICN003
else:
    from typing_extensions import override

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context

SCHEMA = PropertiesList(
    Property("id", IntegerType, required=True),
    Property("updated_at", DateTimeType, required=True),
    Property("value", StringType),
).to_dict()


def _make_tap() -> Tap:
    class _Tap(Tap):
        name = "test-tap"
        config_jsonschema: t.ClassVar[dict] = {}

        def discover_streams(self) -> list:
            return []

    return _Tap(config={})


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def tap() -> Tap:
    return _make_tap()


# ---------------------------------------------------------------------------
# FullTableReplication
# ---------------------------------------------------------------------------


class FullTableStream(Stream):
    name = "full_table"
    schema = SCHEMA
    replication = FullTableReplication()

    @override
    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        return []


def test_full_table_replication_method(tap: Tap) -> None:
    stream = FullTableStream(tap)
    assert stream.replication_method == REPLICATION_FULL_TABLE


def test_full_table_replication_key_is_none(tap: Tap) -> None:
    stream = FullTableStream(tap)
    assert stream.replication_key is None


def test_full_table_is_sorted_is_false(tap: Tap) -> None:
    stream = FullTableStream(tap)
    assert stream.is_sorted is False


def test_full_table_forced_override_still_wins(tap: Tap) -> None:
    """forced_replication_method from catalog still takes top priority."""
    stream = FullTableStream(tap)
    stream.forced_replication_method = REPLICATION_INCREMENTAL
    assert stream.replication_method == REPLICATION_INCREMENTAL


# ---------------------------------------------------------------------------
# IncrementalReplication — defaults
# ---------------------------------------------------------------------------


class IncrementalStream(Stream):
    name = "incremental"
    schema = SCHEMA
    replication = IncrementalReplication(key="updated_at")

    @override
    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        return []


def test_incremental_replication_method(tap: Tap) -> None:
    stream = IncrementalStream(tap)
    assert stream.replication_method == REPLICATION_INCREMENTAL


def test_incremental_replication_key(tap: Tap) -> None:
    stream = IncrementalStream(tap)
    assert stream.replication_key == "updated_at"


def test_incremental_is_sorted_default_false(tap: Tap) -> None:
    stream = IncrementalStream(tap)
    assert stream.is_sorted is False


def test_incremental_check_sorted_default_true(tap: Tap) -> None:
    stream = IncrementalStream(tap)
    assert stream.check_sorted is True


def test_incremental_state_comparator_default(tap: Tap) -> None:
    stream = IncrementalStream(tap)
    assert isinstance(stream.state_manager._comparator, AscendingComparator)


# ---------------------------------------------------------------------------
# IncrementalReplication — custom options
# ---------------------------------------------------------------------------


class SortedIncrementalStream(Stream):
    name = "sorted_incremental"
    schema = SCHEMA
    replication = IncrementalReplication(
        key="updated_at",
        is_sorted=True,
        check_sorted=False,
        state_comparator=StrictAscendingComparator(),
    )

    @override
    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        return []


def test_sorted_incremental_is_sorted(tap: Tap) -> None:
    stream = SortedIncrementalStream(tap)
    assert stream.is_sorted is True


def test_sorted_incremental_check_sorted_false(tap: Tap) -> None:
    stream = SortedIncrementalStream(tap)
    assert stream.check_sorted is False


def test_sorted_incremental_state_comparator(tap: Tap) -> None:
    stream = SortedIncrementalStream(tap)
    assert isinstance(stream.state_manager._comparator, StrictAscendingComparator)


# ---------------------------------------------------------------------------
# LogBasedReplication
# ---------------------------------------------------------------------------


class LogBasedStream(Stream):
    name = "log_based"
    schema = SCHEMA
    replication = LogBasedReplication()

    @override
    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        return []


def test_log_based_replication_method(tap: Tap) -> None:
    stream = LogBasedStream(tap)
    assert stream.replication_method == REPLICATION_LOG_BASED


# ---------------------------------------------------------------------------
# Catalog override takes priority over replication_config
# ---------------------------------------------------------------------------


def test_catalog_replication_key_overrides_config(tap: Tap) -> None:
    """Instance-level replication_key (from catalog) beats replication_config.key."""
    stream = IncrementalStream(tap)
    stream.replication_key = "id"
    assert stream.replication_key == "id"


def test_catalog_forced_method_overrides_config(tap: Tap) -> None:
    stream = IncrementalStream(tap)
    stream.forced_replication_method = REPLICATION_FULL_TABLE
    assert stream.replication_method == REPLICATION_FULL_TABLE


# ---------------------------------------------------------------------------
# Backward compatibility: flat attributes still work unchanged
# ---------------------------------------------------------------------------


class LegacyIncrementalStream(Stream):
    """Uses the old flat-attribute style — must continue to work."""

    name = "legacy_incremental"
    schema = SCHEMA
    replication_key = "updated_at"

    @override
    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        return []


def test_legacy_flat_replication_key(tap: Tap) -> None:
    stream = LegacyIncrementalStream(tap)
    assert stream.replication_key == "updated_at"


def test_legacy_flat_replication_method(tap: Tap) -> None:
    stream = LegacyIncrementalStream(tap)
    assert stream.replication_method == REPLICATION_INCREMENTAL


def test_legacy_flat_is_sorted_default(tap: Tap) -> None:
    stream = LegacyIncrementalStream(tap)
    assert stream.is_sorted is False


class LegacyFullTableStream(Stream):
    name = "legacy_full_table"
    schema = SCHEMA

    @override
    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        return []


def test_legacy_full_table_no_key(tap: Tap) -> None:
    stream = LegacyFullTableStream(tap)
    assert stream.replication_key is None
    assert stream.replication_method == REPLICATION_FULL_TABLE


# ---------------------------------------------------------------------------
# apply_catalog with replication_config
# ---------------------------------------------------------------------------


def _make_catalog(
    stream_name: str,
    schema: dict,
    replication_method: str | None = None,
    replication_key: str | None = None,
) -> Catalog:
    entry: dict = {
        "tap_stream_id": stream_name,
        "stream": stream_name,
        "schema": schema,
        "metadata": MetadataMapping(),
    }
    if replication_method:
        entry["replication_method"] = replication_method
    if replication_key:
        entry["replication_key"] = replication_key
    return Catalog.from_dict({"streams": [entry]})


def test_apply_catalog_full_table_overrides_incremental(tap: Tap) -> None:
    """Catalog FULL_TABLE replaces IncrementalReplication with FullTableReplication."""
    stream = SortedIncrementalStream(tap)
    assert stream.replication_method == REPLICATION_INCREMENTAL
    assert stream.is_sorted is True

    stream.apply_catalog(
        _make_catalog(
            stream.name,
            stream.schema,
            REPLICATION_FULL_TABLE,
        )
    )

    assert stream.replication_method == REPLICATION_FULL_TABLE
    assert stream.replication_key is None
    assert stream.is_sorted is False
    assert isinstance(stream.replication, FullTableReplication)


def test_apply_catalog_incremental_preserves_is_sorted(tap: Tap) -> None:
    """Catalog key override keeps is_sorted=True from the class-level config."""
    stream = SortedIncrementalStream(tap)
    assert stream.replication.is_sorted is True  # type: ignore[union-attr]

    stream.apply_catalog(
        _make_catalog(
            stream.name,
            stream.schema,
            REPLICATION_INCREMENTAL,
            "id",
        )
    )

    assert stream.replication_method == REPLICATION_INCREMENTAL
    assert stream.replication_key == "id"
    assert stream.is_sorted is True  # preserved


def test_apply_catalog_key_only_override_preserves_config(tap: Tap) -> None:
    """Key-only catalog entry (no method) updates the key and preserves the rest."""
    stream = SortedIncrementalStream(tap)

    stream.apply_catalog(
        _make_catalog(
            stream.name,
            stream.schema,
            replication_key="id",
        )
    )

    assert stream.replication_key == "id"
    assert stream.is_sorted is True  # preserved
    assert stream.replication_method == REPLICATION_INCREMENTAL


def test_apply_catalog_log_based(tap: Tap) -> None:
    """Catalog LOG_BASED replaces config with LogBasedReplication."""
    stream = IncrementalStream(tap)

    stream.apply_catalog(
        _make_catalog(
            stream.name,
            stream.schema,
            REPLICATION_LOG_BASED,
        )
    )

    assert stream.replication_method == REPLICATION_LOG_BASED
    assert isinstance(stream.replication, LogBasedReplication)


def test_apply_catalog_no_override_leaves_config_intact(tap: Tap) -> None:
    """Catalog with no replication fields leaves replication_config unchanged."""
    stream = SortedIncrementalStream(tap)
    original_config = stream.replication

    stream.apply_catalog(_make_catalog(stream.name, stream.schema))

    assert stream.replication is original_config


def test_apply_catalog_old_style_still_uses_forced_method(tap: Tap) -> None:
    """Old-style streams (no replication_config) still set forced_replication_method."""
    stream = LegacyIncrementalStream(tap)
    assert stream.forced_replication_method is None

    stream.apply_catalog(
        _make_catalog(
            stream.name,
            stream.schema,
            REPLICATION_FULL_TABLE,
        )
    )

    assert stream.forced_replication_method == REPLICATION_FULL_TABLE
    assert stream.replication_method == REPLICATION_FULL_TABLE
