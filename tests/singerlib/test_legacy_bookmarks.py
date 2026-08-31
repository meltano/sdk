"""Test the legacy singer-python bookmark helpers."""

from __future__ import annotations

import singer
import singer.bookmarks as bookmarks_


def test_write_and_get_bookmark():
    state: dict = {}
    bookmarks_.write_bookmark(state, "my_stream", "last_id", 42)
    assert bookmarks_.get_bookmark(state, "my_stream", "last_id") == 42
    assert state == {"bookmarks": {"my_stream": {"last_id": 42}}}


def test_get_bookmark_default():
    state: dict = {}
    assert bookmarks_.get_bookmark(state, "my_stream", "last_id") is None
    assert bookmarks_.get_bookmark(state, "my_stream", "last_id", default=0) == 0


def test_clear_bookmark():
    state: dict = {}
    bookmarks_.write_bookmark(state, "my_stream", "last_id", 42)
    bookmarks_.clear_bookmark(state, "my_stream", "last_id")
    assert bookmarks_.get_bookmark(state, "my_stream", "last_id") is None


def test_reset_stream():
    state: dict = {}
    bookmarks_.write_bookmark(state, "my_stream", "last_id", 42)
    bookmarks_.write_bookmark(state, "my_stream", "other", "x")
    bookmarks_.reset_stream(state, "my_stream")
    assert state["bookmarks"]["my_stream"] == {}


def test_offsets():
    state: dict = {}
    bookmarks_.set_offset(state, "my_stream", "page", 3)
    assert bookmarks_.get_offset(state, "my_stream") == {"page": 3}
    bookmarks_.clear_offset(state, "my_stream")
    assert bookmarks_.get_offset(state, "my_stream") == {}


def test_currently_syncing():
    state: dict = {}
    assert bookmarks_.get_currently_syncing(state) is None
    bookmarks_.set_currently_syncing(state, "my_stream")
    assert bookmarks_.get_currently_syncing(state) == "my_stream"
    bookmarks_.set_currently_syncing(state, None)
    assert bookmarks_.get_currently_syncing(state) is None


def test_top_level_reexports():
    state: dict = {}
    singer.write_bookmark(state, "my_stream", "last_id", 42)
    assert singer.get_bookmark(state, "my_stream", "last_id") == 42
