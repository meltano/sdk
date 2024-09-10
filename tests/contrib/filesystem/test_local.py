from __future__ import annotations

import typing as t

from singer_sdk.contrib.filesystem import local

if t.TYPE_CHECKING:
    import pathlib


def test_file_read_text(tmp_path: pathlib.Path):
    """Test reading a file."""

    # Write a test file
    path = tmp_path / "test.txt"
    path.write_text("Hello, world!")

    file = local.LocalFile(path)
    assert file.read_text() == "Hello, world!"


def test_file_read(tmp_path: pathlib.Path):
    """Test reading a file."""

    # Write a test file
    path = tmp_path / "test.txt"
    path.write_text("Hello, world!")

    file = local.LocalFile(path)
    assert file.read(3) == "Hel"


def test_directory_list_contents(tmp_path: pathlib.Path):
    """Test listing a directory."""

    # Create a directory with a file and a root-level file
    dirpath = tmp_path / "test"
    dirpath.mkdir()
    (dirpath / "file.txt").write_text("Hello from a directory!")
    (tmp_path / "file.txt").write_text("Hello from the root!")

    directory = local.LocalDirectory(tmp_path)
    contents = list(directory.list_contents())
    assert len(contents) == 3

    root_file, directory, nested_file = contents
    assert isinstance(root_file, local.LocalFile)
    assert root_file.path.name == "file.txt"
    assert root_file.read_text() == "Hello from the root!"

    assert isinstance(directory, local.LocalDirectory)
    assert directory.path.name == "test"

    assert isinstance(nested_file, local.LocalFile)
    assert nested_file.path.name == "file.txt"
    assert nested_file.read_text() == "Hello from a directory!"
