"""Test that ``singer_sdk.singerlib`` is a faithful alias for ``singer``."""

from __future__ import annotations

import importlib

import pytest

import singer
import singer.messages
import singer_sdk.singerlib
import singer_sdk.singerlib.messages


@pytest.mark.parametrize(
    "name",
    sorted(
        set(singer_sdk.singerlib.__all__)
        # `exceptions` is a distinct alias submodule, not the same object.
        # `write_message` on `singer` is a delegator to `singer.messages`,
        # while the shim keeps re-exporting `singer.messages.write_message`
        # directly, for identity with the underlying writer.
        - {"exceptions", "write_message"},
    ),
)
def test_top_level_identity(name: str):
    """Every name exported by the shim is the same object as in ``singer``."""
    assert getattr(singer_sdk.singerlib, name) is getattr(singer, name)


def test_write_message_delegates():
    """`singer.write_message` and the shim both reach the same writer."""
    assert singer_sdk.singerlib.messages.write_message is singer.messages.write_message


@pytest.mark.parametrize(
    "module",
    [
        "singer_sdk.singerlib.catalog",
        "singer_sdk.singerlib.encoding",
        "singer_sdk.singerlib.encoding.base",
        "singer_sdk.singerlib.encoding.simple",
        "singer_sdk.singerlib.exceptions",
        "singer_sdk.singerlib.json",
        "singer_sdk.singerlib.messages",
        "singer_sdk.singerlib.schema",
        "singer_sdk.singerlib.utils",
    ],
)
def test_submodule_identity(module: str):
    """Every shim submodule re-exports the same objects as its counterpart."""
    shim = importlib.import_module(module)
    target = importlib.import_module(module.replace("singer_sdk.singerlib", "singer"))
    for name in shim.__all__:
        assert getattr(shim, name) is getattr(target, name), name


def test_record_message_identity():
    assert singer_sdk.singerlib.RecordMessage is singer.RecordMessage
    assert singer_sdk.singerlib.messages.WRITER is singer.messages.WRITER
