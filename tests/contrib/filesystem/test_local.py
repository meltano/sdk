from __future__ import annotations

import datetime
import typing as t

import pytest

from singer_sdk.contrib.filesystem import local

if t.TYPE_CHECKING:
    import pathlib

    from pytest_subtests import SubTests


class TestLocalFilesystem:
    @pytest.fixture
    def filesystem(self, tmp_path: pathlib.Path) -> local.LocalFileSystem:
        return local.LocalFileSystem(tmp_path)

    def test_file_read(self, filesystem: local.LocalFileSystem):
        """Test reading a file."""

        # Write a test file
        path = filesystem.root / "test.txt"
        path.write_text("Hello, world!")

        with filesystem.open("test.txt") as file:
            assert file.read() == b"Hello, world!"

    def test_file_read_write(
        self,
        filesystem: local.LocalFileSystem,
        subtests: SubTests,
    ):
        """Test writing a file."""

        with subtests.test("write"), filesystem.open("test.txt", mode="wb") as file:
            file.write(b"Hello, world!")

        with subtests.test("read"), filesystem.open("test.txt") as file:
            assert file.read() == b"Hello, world!"

    def test_file_creation_time(self, filesystem: local.LocalFileSystem):
        """Test getting the creation time of a file."""

        with filesystem.open("test.txt", mode="wb") as file:
            file.write(b"Hello, world!")

        assert isinstance(filesystem.created("test.txt"), datetime.datetime)

    def test_file_modified_time(self, filesystem: local.LocalFileSystem):
        """Test getting the last modified time of a file."""

        with filesystem.open("test.txt", mode="wb") as file:
            file.write(b"Hello, world!")

        assert isinstance(filesystem.modified("test.txt"), datetime.datetime)
