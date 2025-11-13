"""Module for SQL-related operations."""

from __future__ import annotations

import typing as t
import warnings

from singer_sdk.helpers._compat import SingerSDKDeprecationWarning

if t.TYPE_CHECKING:
    from singer_sdk.sql.connector import SQLConnector  # noqa: F401


def __getattr__(name: str) -> t.Any:  # noqa: ANN401
    """Provide backward compatibility for moved SQL classes.

    Args:
        name: The name of the attribute to import.

    Returns:
        The imported attribute.

    Raises:
        AttributeError: If the attribute is not found.
    """
    if name == "SQLConnector":
        warnings.warn(
            f"Importing {name} from singer_sdk.connectors is deprecated. "
            f"Please import from singer_sdk.sql instead: "
            f"from singer_sdk.sql import {name}",
            SingerSDKDeprecationWarning,
            stacklevel=2,
        )
        from singer_sdk.sql import SQLConnector  # noqa: PLC0415

        return SQLConnector

    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)
