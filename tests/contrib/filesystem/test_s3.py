from __future__ import annotations

import typing as t

import moto
import pytest

from singer_sdk.contrib.filesystem import s3 as s3fs


class TestS3Filesystem:
    @pytest.fixture
    def filesystem(self) -> t.Generator[s3fs.S3FileSystem, None, None]:
        bucket = "test-bucket"
        prefix = "path/to/dir/"
        with moto.mock_aws():
            fs = s3fs.S3FileSystem(bucket=bucket, prefix=prefix)
            fs.client.create_bucket(Bucket=bucket)
            yield fs

    @pytest.mark.xfail
    def test_file_read_write(self, filesystem: s3fs.S3FileSystem):
        """Test reading a file."""

        # Write a test file
        filesystem.client.put_object(
            Bucket=filesystem.bucket,
            Key=f"{filesystem.prefix}/test.txt",
            Body=b"Hello, world!",
        )

        with filesystem.open("test.txt") as file:
            assert file.read() == b"Hello, world!"
