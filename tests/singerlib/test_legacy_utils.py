"""Test the legacy singer-python utils additions."""

from __future__ import annotations

import json
from datetime import timezone

import pytest

import singer.utils as utils_
from singer.catalog import Catalog


def test_now_is_utc():
    result = utils_.now()
    assert result.tzinfo is timezone.utc


def test_load_json(tmp_path):
    path = tmp_path / "data.json"
    path.write_text(json.dumps({"a": 1}))
    assert utils_.load_json(path) == {"a": 1}


def test_check_config_passes():
    utils_.check_config({"a": 1, "b": 2}, ["a", "b"])


def test_check_config_raises_on_missing():
    with pytest.raises(utils_.ConfigKeyError) as exc_info:
        utils_.check_config({"a": 1}, ["a", "b"])
    assert exc_info.value.missing_keys == ["b"]


def test_parse_args(tmp_path, monkeypatch: pytest.MonkeyPatch):
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps({"api_key": "abc"}))

    state_path = tmp_path / "state.json"
    state_path.write_text(json.dumps({"bookmarks": {}}))

    catalog_path = tmp_path / "catalog.json"
    catalog_path.write_text(json.dumps({"streams": []}))

    monkeypatch.setattr(
        "sys.argv",
        [
            "tap",
            "--config",
            str(config_path),
            "--state",
            str(state_path),
            "--catalog",
            str(catalog_path),
        ],
    )

    args = utils_.parse_args(["api_key"])
    assert args.config == {"api_key": "abc"}
    assert args.state == {"bookmarks": {}}
    assert isinstance(args.catalog, Catalog)
    assert args.config_path == str(config_path)


def test_parse_args_missing_required_key(tmp_path, monkeypatch: pytest.MonkeyPatch):
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps({}))

    monkeypatch.setattr("sys.argv", ["tap", "--config", str(config_path)])

    with pytest.raises(utils_.ConfigKeyError):
        utils_.parse_args(["api_key"])


def test_parse_args_no_properties_flag(tmp_path, monkeypatch: pytest.MonkeyPatch):
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps({}))

    monkeypatch.setattr(
        "sys.argv",
        ["tap", "--config", str(config_path), "--properties", "x"],
    )

    with pytest.raises(SystemExit):
        utils_.parse_args([])
