"""JSON Schema for each filesystem configuration."""

from __future__ import annotations

from singer_sdk import typing as th  # JSON schema typing helpers

# https://filesystem-spec.readthedocs.io/en/latest/api.html#fsspec.implementations.ftp.FTPFileSystem
FTP = th.Property(
    "ftp",
    th.ObjectType(
        th.Property(
            "host",
            th.StringType,
            required=True,
            title="FTP Host",
            description="FTP server host",
        ),
        th.Property(
            "port",
            th.IntegerType,
            default=21,
            title="FTP Port",
            description="FTP server port",
        ),
        th.Property(
            "username",
            th.StringType,
            title="FTP Username",
            description="FTP username",
        ),
        th.Property(
            "password",
            th.StringType,
            secret=True,
            title="FTP Password",
            description="FTP password",
        ),
        th.Property(
            "timeout",
            th.IntegerType,
            default=60,
            title="Timeout",
            description="Timeout of the FTP connection in seconds",
        ),
        th.Property(
            "encoding",
            th.StringType,
            default="utf-8",
            title="Encoding",
            description="FTP server encoding",
        ),
    ),
    description="FTP connection settings",
)


# https://filesystem-spec.readthedocs.io/en/latest/api.html#fsspec.implementations.sftp.SFTPFileSystem
SFTP = th.Property(
    "sftp",
    th.ObjectType(
        th.Property(
            "host",
            th.StringType,
            required=True,
            title="SFTP Host",
            description="SFTP server host",
        ),
        th.Property(
            "ssh_kwargs",
            th.ObjectType(
                th.Property(
                    "port",
                    th.IntegerType,
                    default=22,
                    title="SFTP Port",
                    description="SFTP server port",
                ),
                th.Property(
                    "username",
                    th.StringType,
                    required=True,
                    title="SFTP Username",
                    description="SFTP username",
                ),
                th.Property(
                    "password",
                    th.StringType,
                    secret=True,
                    title="SFTP Password",
                    description="SFTP password",
                ),
                th.Property(
                    "pkey",
                    th.StringType,
                    secret=True,
                    title="SFTP Private Key",
                    description="Private key",
                ),
                th.Property(
                    "timeout",
                    th.IntegerType,
                    default=60,
                    title="Timeout",
                    description="Timeout of the SFTP connection in seconds",
                ),
            ),
            title="SSH Connection Settings",
            description="SSH connection settings",
        ),
    ),
    description="SFTP connection settings",
)
