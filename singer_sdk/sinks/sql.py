"""Deprecated SQL sink module.

.. deprecated:: Next Release
    Import from singer_sdk.sql instead.
"""

from __future__ import annotations

import typing as t
import warnings

from singer_sdk.helpers._compat import SingerSDKDeprecationWarning

if t.TYPE_CHECKING:
    from singer_sdk.sql import SQLSink  # noqa: F401


def __getattr__(name: str) -> t.Any:  # noqa: ANN401
    """Provide backward compatibility for moved SQL classes.

    Args:
        name: The name of the attribute to import.

    Returns:
        The imported attribute.

    Raises:
        AttributeError: If the attribute is not found.
    """
    if name == "SQLSink":
        warnings.warn(
            f"Importing {name} from singer_sdk.sinks.sql is deprecated. "
            f"Please import from singer_sdk.sql instead.",
            SingerSDKDeprecationWarning,
            stacklevel=2,
        )
        from singer_sdk.sql import SQLSink  # noqa: PLC0415

        return SQLSink

    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)
