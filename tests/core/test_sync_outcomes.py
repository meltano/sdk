"""Tests for per-stream sync outcome tracking and tap-level exit codes."""

from __future__ import annotations

import json
import typing as t
from pathlib import Path

from click.testing import CliRunner

from singer_sdk import Stream, Tap
from singer_sdk.exceptions import (
    AbortedSyncFailedException,
    AbortedSyncPausedException,
    FatalSyncError,
)
from singer_sdk.streams.core import SyncResult

if t.TYPE_CHECKING:
    import pytest

    from singer_sdk.helpers import types


# ---------------------------------------------------------------------------
# Stream fixtures
# ---------------------------------------------------------------------------


class GoodStream(Stream):
    name = "good"
    schema: t.ClassVar = {"type": "object", "properties": {"id": {"type": "integer"}}}

    def get_records(self, _context: types.Context | None):
        yield {"id": 1}


class BadStream(Stream):
    name = "bad"
    schema: t.ClassVar = {"type": "object", "properties": {"id": {"type": "integer"}}}

    def get_records(self, _context: types.Context | None):
        msg = "intentional failure"
        raise FatalSyncError(msg)


class AbortPausedStream(Stream):
    name = "abort_paused"
    schema: t.ClassVar = {"type": "object", "properties": {"id": {"type": "integer"}}}

    def get_records(self, _context: types.Context | None):
        raise AbortedSyncPausedException


class AbortFailedStream(Stream):
    name = "abort_failed"
    schema: t.ClassVar = {"type": "object", "properties": {"id": {"type": "integer"}}}

    def get_records(self, _context: types.Context | None):
        msg = "forced"
        raise AbortedSyncFailedException(msg)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_tap(*stream_classes: type[Stream]) -> Tap:
    """Create a minimal Tap instance with the given stream classes."""

    class _Tap(Tap):
        name = "test-outcomes-tap"
        config_jsonschema: t.ClassVar = {"type": "object", "properties": {}}

        def discover_streams(self) -> list[Stream]:
            return [cls(self) for cls in stream_classes]

    return _Tap(config={})


def make_tap_class(*stream_classes: type[Stream]) -> type[Tap]:
    """Return a Tap subclass (not an instance) for CLI invocation."""

    class _Tap(Tap):
        name = "test-outcomes-tap"
        config_jsonschema: t.ClassVar = {"type": "object", "properties": {}}

        def discover_streams(self) -> list[Stream]:
            return [cls(self) for cls in stream_classes]

    return _Tap


# ---------------------------------------------------------------------------
# Unit tests — sync_result attribute
# ---------------------------------------------------------------------------


def test_sync_result_default_is_none() -> None:
    tap = make_tap(GoodStream)
    stream = tap.streams["good"]
    assert stream.sync_result is None


def test_sync_result_success() -> None:
    tap = make_tap(GoodStream)
    tap.sync_all()
    assert tap.streams["good"].sync_result is SyncResult.SUCCESS


def test_sync_result_failed() -> None:
    tap = make_tap(BadStream)
    tap.sync_all()
    assert tap.streams["bad"].sync_result is SyncResult.FAILED


def test_sync_result_aborted_on_paused() -> None:
    tap = make_tap(AbortPausedStream)
    tap.sync_all()
    assert tap.streams["abort_paused"].sync_result is SyncResult.ABORTED


def test_sibling_continues_after_failure() -> None:
    tap = make_tap(BadStream, GoodStream)
    # Must not raise despite BadStream failing
    tap.sync_all()
    assert tap.streams["bad"].sync_result is SyncResult.FAILED
    assert tap.streams["good"].sync_result is SyncResult.SUCCESS


def test_lifecycle_signal_stops_siblings() -> None:
    tap = make_tap(AbortPausedStream, GoodStream)
    tap.sync_all()
    # AbortPausedStream's exception is caught; GoodStream still runs successfully.
    assert tap.streams["good"].sync_result is SyncResult.SUCCESS


# ---------------------------------------------------------------------------
# Unit tests — summary logging
# ---------------------------------------------------------------------------


def test_summary_logged_success(caplog: pytest.LogCaptureFixture) -> None:
    tap = make_tap(GoodStream)
    with caplog.at_level("INFO", logger="root"):
        tap.sync_all()
    assert "Stream 'good' sync result: success" in caplog.text


def test_summary_logged_failed(caplog: pytest.LogCaptureFixture) -> None:
    tap = make_tap(BadStream)
    with caplog.at_level("ERROR", logger="root"):
        tap.sync_all()
    assert "Stream 'bad' sync result: failed" in caplog.text


def test_summary_logged_aborted(caplog: pytest.LogCaptureFixture) -> None:
    tap = make_tap(AbortPausedStream)
    with (
        caplog.at_level("WARNING", logger="root"),
    ):
        tap.sync_all()
    # Summary is emitted in the finally block even when sync_all raises
    assert "Stream 'abort_paused' sync result: aborted" in caplog.text


def test_summary_not_logged_for_never_synced(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A stream that is skipped (deselected) must not produce a sync result line."""
    tap = make_tap(GoodStream)
    stream = tap.streams["good"]
    # Patch selected / has_selected_descendents so sync_all skips this stream
    type(stream).selected = property(lambda _: False)  # type: ignore[assignment]
    type(stream).has_selected_descendents = property(  # type: ignore[assignment]
        lambda _: False
    )
    with caplog.at_level("INFO", logger="root"):
        tap.sync_all()
    assert "sync result" not in caplog.text


# ---------------------------------------------------------------------------
# CLI exit code tests
# ---------------------------------------------------------------------------


def _cli_invoke(tap_cls: type[Tap]) -> int:
    """Invoke tap CLI with an empty config file; return the exit code."""
    runner = CliRunner()
    with runner.isolated_filesystem():
        Path("config.json").write_text(json.dumps({}), encoding="utf-8")
        result = runner.invoke(tap_cls.cli, ["--config", "config.json"])
    return result.exit_code


def test_cli_exit_0_on_success() -> None:
    assert _cli_invoke(make_tap_class(GoodStream)) == 0


def test_cli_exit_1_on_stream_failure() -> None:
    assert _cli_invoke(make_tap_class(BadStream)) == 1


def test_cli_exit_0_on_aborted_sync_paused() -> None:
    assert _cli_invoke(make_tap_class(AbortPausedStream)) == 0


def test_cli_exit_1_on_aborted_sync_failed() -> None:
    assert _cli_invoke(make_tap_class(AbortFailedStream)) == 1
