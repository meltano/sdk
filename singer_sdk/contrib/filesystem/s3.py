"""S3 filesystem operations."""

from __future__ import annotations

import io
import typing as t
from datetime import datetime

from singer_sdk.contrib.filesystem import base

if t.TYPE_CHECKING:
    from datetime import datetime

    from mypy_boto3_s3.client import S3Client

__all__ = ["S3File", "S3FileSystem"]


class S3File(base.AbstractFile):
    """S3 file operations."""

    def seekable(self) -> bool:  # noqa: PLR6301
        """Check if the file is seekable.

        Returns:
            False
        """
        return False

    def __iter__(self) -> t.Iterator[str]:  # noqa: D105
        return iter(self.buffer)


class S3FileSystem(base.AbstractFileSystem):
    """S3 file system operations."""

    def __init__(  # noqa: D107
        self,
        *,
        bucket: str,
        prefix: str = "",
        region: str | None = None,
        endpoint_url: str | None = None,
    ):
        super().__init__()
        self._bucket = bucket
        self._prefix = prefix
        self._region = region
        self._endpoint_url = endpoint_url

        self._client: S3Client | None = None

    @property
    def client(self) -> S3Client:
        """Get the S3 client."""
        if self._client is None:
            import boto3  # noqa: PLC0415

            self._client = boto3.client(
                "s3",
                region_name=self._region,
                endpoint_url=self._endpoint_url,
            )

        return self._client

    @property
    def bucket(self) -> str:  # noqa: D102
        return self._bucket

    @property
    def prefix(self) -> str:  # noqa: D102
        return self._prefix

    def open(  # noqa: D102
        self,
        filename: str,
        *,
        mode: base.FileMode = base.FileMode.read,
    ) -> S3File:
        key = f"{self.prefix}{filename}"
        if mode == base.FileMode.read:
            response = self._client.get_object(Bucket=self._bucket, Key=key)
            return S3File(response["Body"], filename)
        if mode == base.FileMode.write:
            buffer = io.BytesIO()
            self._client.upload_fileobj(buffer, self._bucket, key)
            return S3File(buffer, filename)

        msg = "Only read mode is supported."
        raise ValueError(msg)

    def modified(self, filename: str) -> datetime:  # noqa: D102
        response = self._client.get_object_attributes(Bucket=self._bucket, Key=filename)
        return response["LastModified"]

    def created(self, filename: str) -> datetime:  # noqa: ARG002, D102, PLR6301
        msg = "S3 does not support file creation time."
        raise NotImplementedError(msg)
