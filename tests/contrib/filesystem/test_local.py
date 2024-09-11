from __future__ import annotations

import datetime
import typing as t
import unittest.mock

from singer_sdk.contrib.filesystem import local

if t.TYPE_CHECKING:
    import pathlib


class TestLocalFilesystem:
    def test_file_read_text(self, tmp_path: pathlib.Path):
        """Test reading a file."""

        # Write a test file
        path = tmp_path / "test.txt"
        path.write_text("Hello, world!")

        file = local.LocalFile(path)
        assert file.read_text() == "Hello, world!"

    def test_file_read(self, tmp_path: pathlib.Path):
        """Test reading a file."""

        # Write a test file
        path = tmp_path / "test.txt"
        path.write_text("Hello, world!")

        file = local.LocalFile(path)
        assert file.read(3) == "Hel"

    def test_file_creation_time(self, tmp_path: pathlib.Path):
        """Test getting the creation time of a file."""

        path = tmp_path / "test.txt"
        path.write_text("Hello, world!")

        file = local.LocalFile(path)
        assert isinstance(file.creation_time, datetime.datetime)

        with unittest.mock.patch("pathlib.Path.stat") as mock_stat:
            ts = 1704067200
            mock_stat.return_value = unittest.mock.Mock(st_birthtime=ts, st_ctime=ts)
            assert file.creation_time.timestamp() == ts

    def test_file_modified_time(self, tmp_path: pathlib.Path):
        """Test getting the last modified time of a file."""

        path = tmp_path / "test.txt"
        path.write_text("Hello, world!")

        file = local.LocalFile(path)
        assert isinstance(file.modified_time, datetime.datetime)

        with unittest.mock.patch("pathlib.Path.stat") as mock_stat:
            ts = 1704067200
            mock_stat.return_value = unittest.mock.Mock(st_mtime=ts)
            assert file.modified_time.timestamp() == ts

    def test_directory_list_contents(self, tmp_path: pathlib.Path):
        """Test listing a directory."""

        # Create a directory with a file and a root-level file
        (tmp_path / "a.txt").write_text("Hello from the root!")
        dirpath = tmp_path / "b"
        dirpath.mkdir()
        (dirpath / "c.txt").write_text("Hello from a directory!")

        directory = local.LocalDirectory(tmp_path)
        contents = list(directory.list_contents())
        assert len(contents) == 3

        # Get the root file, the directory, and the nested file regardless of order
        root_file, directory, nested_file = sorted(contents, key=lambda x: x.path.name)
        assert isinstance(root_file, local.LocalFile)
        assert root_file.path.name == "a.txt"
        assert root_file.read_text() == "Hello from the root!"

        assert isinstance(directory, local.LocalDirectory)
        assert directory.path.name == "b"

        assert isinstance(nested_file, local.LocalFile)
        assert nested_file.path.name == "c.txt"
        assert nested_file.read_text() == "Hello from a directory!"
