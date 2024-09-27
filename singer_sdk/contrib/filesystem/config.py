"""JSON Schema for each filesystem configuration."""

from __future__ import annotations

from singer_sdk import typing as th  # JSON schema typing helpers

FTP = th.Property(
    "ftp",
    th.ObjectType(
        th.Property(
            "host",
            th.StringType,
            required=True,
            description="FTP server host",
        ),
        th.Property(
            "port",
            th.IntegerType,
            default=21,
            description="FTP server port",
        ),
        th.Property(
            "username",
            th.StringType,
            description="FTP username",
        ),
        th.Property(
            "password",
            th.StringType,
            secret=True,
            description="FTP password",
        ),
        th.Property(
            "timeout",
            th.IntegerType,
            default=60,
            description="Timeout of the FTP connection in seconds",
        ),
        th.Property(
            "encoding",
            th.StringType,
            default="utf-8",
            description="FTP server encoding",
        ),
    ),
    description="FTP connection settings",
)


SFTP = th.Property(
    "sftp",
    th.ObjectType(
        th.Property(
            "host",
            th.StringType,
            required=True,
            description="SFTP server host",
        ),
        th.Property(
            "ssh_kwargs",
            th.ObjectType(
                th.Property(
                    "port",
                    th.IntegerType,
                    default=22,
                    description="SFTP server port",
                ),
                th.Property(
                    "username",
                    th.StringType,
                    required=True,
                    description="SFTP username",
                ),
                th.Property(
                    "password",
                    th.StringType,
                    secret=True,
                    description="SFTP password",
                ),
                th.Property(
                    "pkey",
                    th.StringType,
                    secret=True,
                    description="Private key",
                ),
                th.Property(
                    "timeout",
                    th.IntegerType,
                    default=60,
                    description="Timeout of the SFTP connection in seconds",
                ),
            ),
            description="SSH connection settings",
        ),
    ),
    description="SFTP connection settings",
)
