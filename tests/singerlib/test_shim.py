"""Test that ``singer_sdk.singerlib`` is a faithful alias for ``singer``."""

from __future__ import annotations

import importlib

import pytest

import singer
import singer.messages
import singer_sdk.singerlib


@pytest.mark.parametrize(
    "name",
    sorted(set(singer_sdk.singerlib.__all__) - {"exceptions"}),
)
def test_top_level_identity(name: str):
    """Every name exported by the shim is the same object as in ``singer``."""
    assert getattr(singer_sdk.singerlib, name) is getattr(singer, name)


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
