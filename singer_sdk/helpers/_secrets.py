"""Helpers function for secrets management."""

from __future__ import annotations

COMMON_SECRET_KEYS = [
    "db_password",
    "password",
    "access_key",
    "private_key",
    "client_id",
    "client_secret",
    "refresh_token",
    "access_token",
]
COMMON_SECRET_KEY_SUFFIXES = ["access_key_id"]


def is_common_secret_key(key_name: str) -> bool:
    """Return true if the key_name value matches a known secret name or pattern."""
    if key_name in COMMON_SECRET_KEYS:
        return True
    return any(
        key_name.lower().endswith(key_suffix)
        for key_suffix in COMMON_SECRET_KEY_SUFFIXES
    )


class SecretString(str):
    """For now, this class wraps a sensitive string to be identified as such later."""

    def __init__(self, contents: str) -> None:
        """Initialize secret string."""
        self.contents = contents

    def __repr__(self) -> str:
        """Render secret contents."""
        return str(self.contents.__repr__())

    def __str__(self) -> str:
        """Render secret contents."""
        return str(self.contents.__str__())
