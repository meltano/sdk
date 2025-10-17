"""SDK for building Singer taps."""

from __future__ import annotations

import typing as t
import warnings

from singer_sdk.helpers._compat import SingerSDKDeprecationWarning
from singer_sdk.streams.core import Stream
from singer_sdk.streams.graphql import GraphQLStream
from singer_sdk.streams.rest import RESTStream

__all__ = ["GraphQLStream", "RESTStream", "Stream"]


def __getattr__(name: str) -> t.Any:  # noqa: ANN401
    """Provide backward compatibility for moved SQL classes.

    Args:
        name: The name of the attribute to import.

    Returns:
        The imported attribute.

    Raises:
        AttributeError: If the attribute is not found.
    """
    if name == "SQLStream":
        warnings.warn(
            f"Importing {name} from singer_sdk.streams is deprecated. "
            f"Please import from singer_sdk.sql instead: "
            f"from singer_sdk.sql import {name}",
            SingerSDKDeprecationWarning,
            stacklevel=2,
        )
        from singer_sdk.sql import SQLStream  # noqa: PLC0415

        return SQLStream

    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)
