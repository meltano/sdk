"""Helper functions, helper classes, and decorators."""

from typing import Union


COMMON_SECRET_KEYS = ["db_password", "password", "access_key", "private_key"]
COMMON_SECRET_KEY_SUFFIXES = ["access_key_id"]


def is_common_secret_key(key_name: str) -> bool:
    """Return true if the key_name value matches a known secret name or pattern."""
    if key_name in COMMON_SECRET_KEYS:
        return True
    if any(
        [
            key_name.lower().endswith(key_suffix)
            for key_suffix in COMMON_SECRET_KEY_SUFFIXES
        ]
    ):
        return True
    return False


def listify(str_or_list: Union[str, list]) -> list:
    """Take a string or list and return it as a list."""
    if isinstance(str_or_list, str):
        result = str_or_list.split(",")
    else:
        result = str_or_list
    return result


class SecretString(str):
    """For now, this class wraps a sensitive string to be identified as such later."""

    def __init__(self, contents):
        self.contents = contents

    def __repr__(self) -> str:
        return self.contents.__repr__()

    def __str__(self) -> str:
        return self.contents.__str__()


class classproperty(property):
    def __get__(self, obj, objtype=None):
        return super(classproperty, self).__get__(objtype)

    def __set__(self, obj, value):
        super(classproperty, self).__set__(type(obj), value)

    def __delete__(self, obj):
        super(classproperty, self).__delete__(type(obj))
