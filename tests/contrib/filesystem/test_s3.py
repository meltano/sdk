from __future__ import annotations

import datetime
import typing as t

import boto3
import moto
import pytest

from singer_sdk.contrib.filesystem import s3 as s3fs

if t.TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client


class TestS3Filesystem:
    @pytest.fixture
    def client(self) -> t.Generator[S3Client, None, None]:
        with moto.mock_aws():
            yield boto3.client("s3")

    @pytest.fixture
    def bucket(self, client: S3Client) -> str:
        """Return the name of a bucket."""
        value = "test-bucket"
        client.create_bucket(Bucket=value)
        return "test-bucket"

    def test_file_read_text(self, client: S3Client, bucket: str):
        """Test reading a file."""
        client.put_object(Bucket=bucket, Key="test.txt", Body=b"Hello, world!")

        file = s3fs.S3File(client, bucket=bucket, key="test.txt")
        assert file.read_text() == "Hello, world!"

    def test_file_read(self, client: S3Client, bucket: str):
        """Test reading a file."""
        client.put_object(Bucket=bucket, Key="test.txt", Body=b"Hello, world!")

        file = s3fs.S3File(client, bucket=bucket, key="test.txt")
        assert file.read(3) == "Hel"

    def test_file_modified_time(self, client: S3Client, bucket: str):
        """Test getting the last modified time of a file."""
        client.put_object(Bucket=bucket, Key="test.txt", Body=b"Hello, world!")

        file = s3fs.S3File(client, bucket=bucket, key="test.txt")
        assert file.modified_time is not None
        assert isinstance(file.modified_time, datetime.datetime)

    def test_root_list_contents(self, client: S3Client, bucket: str):
        """Test listing a directory."""
        prefix = "path/to/dir/"
        key = "path/to/dir/test.txt"

        client.put_object(Bucket=bucket, Key=key, Body=b"Hello, world!")
        fs = s3fs.S3FileSystem(client, bucket=bucket, prefix=prefix)

        contents = list(fs.root.list_contents())
        assert len(contents) == 1
        assert isinstance(contents[0], s3fs.S3File)
        assert contents[0]._key == "path/to/dir/test.txt"
        assert contents[0].read_text() == "Hello, world!"
