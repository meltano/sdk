"""S3 filesystem operations."""

from __future__ import annotations

import typing as t

from singer_sdk.contrib.filesystem import base

if t.TYPE_CHECKING:
    from datetime import datetime

    from mypy_boto3_s3.client import S3Client
    from mypy_boto3_s3.type_defs import GetObjectOutputTypeDef

__all__ = ["S3Directory", "S3File", "S3FileSystem"]


class S3File(base.AbstractFile):
    """S3 file operations."""

    def __init__(self, client: S3Client, bucket: str, key: str):
        """Create a new S3File instance."""
        self._client = client
        self._bucket = bucket
        self._key = key
        self._s3_object: GetObjectOutputTypeDef | None = None

    def __repr__(self) -> str:
        """A string representation of the S3File.

        Returns:
            A string representation of the S3File.
        """
        return f"S3File({self._key})"

    @property
    def s3_object(self) -> GetObjectOutputTypeDef:
        """Get the S3 object."""
        if self._s3_object is None:
            self._s3_object = self._client.get_object(
                Bucket=self._bucket,
                Key=self._key,
            )

        return self._s3_object

    def read(self, size: int = -1) -> bytes:
        """Read the file contents.

        Args:
            size: Number of bytes to read. If not specified, the entire file is read.

        Returns:
            The file contents as a string.
        """
        data = self.s3_object["Body"].read(amt=size)

        # Clear the cache so that the next read will re-fetch the object
        self._s3_object = None
        return data

    @property
    def modified_time(self) -> datetime:
        """Get the last modified time of the file.

        Returns:
            The last modified time of the file.
        """
        return self.s3_object["LastModified"]


class S3Directory(base.AbstractDirectory[S3File]):
    """S3 directory operations."""

    def __init__(self, client: S3Client, bucket: str, prefix: str):
        """Create a new S3Directory instance."""
        self._client = client
        self._bucket = bucket
        self._prefix = prefix

    def __repr__(self) -> str:
        """A string representation of the S3Directory.

        Returns:
            A string representation of the S3Directory.
        """
        return f"S3Directory({self._prefix})"

    def list_contents(self) -> t.Generator[S3File | S3Directory, None, None]:
        """List files in the directory.

        Yields:
            A file or directory node
        """
        paginator = self._client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self._bucket, Prefix=self._prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.endswith("/"):
                    yield S3Directory(self._client, self._bucket, key)
                else:
                    yield S3File(self._client, self._bucket, key)


class S3FileSystem(base.AbstractFileSystem):
    """S3 file system operations."""

    def __init__(self, client: S3Client, *, bucket: str, prefix: str):
        """Create a new S3FileSystem instance."""
        super().__init__()
        self._client = client
        self._bucket = bucket
        self._prefix = prefix

    @property
    def root(self) -> S3Directory:
        """Get the root directory."""
        return S3Directory(self._client, self._bucket, self._prefix)
