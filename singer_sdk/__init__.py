"""SDK for building Singer taps."""

from __future__ import annotations

import typing as t
import warnings

from singer_sdk import streams
from singer_sdk.helpers._compat import SingerSDKDeprecationWarning
from singer_sdk.mapper_base import InlineMapper
from singer_sdk.plugin_base import PluginBase
from singer_sdk.schema.source import (
    OpenAPISchema,
    SchemaDirectory,
    SchemaSource,
    StreamSchema,
)
from singer_sdk.sinks import BatchSink, RecordSink, Sink
from singer_sdk.streams import GraphQLStream, RESTStream, Stream
from singer_sdk.tap_base import Tap
from singer_sdk.target_base import Target

if t.TYPE_CHECKING:
    from singer_sdk.sql import (  # noqa: F401
        SQLConnector,
        SQLSink,
        SQLStream,
        SQLTap,
        SQLTarget,
    )

__all__ = [
    "BatchSink",
    "GraphQLStream",
    "InlineMapper",
    "OpenAPISchema",
    "PluginBase",
    "RESTStream",
    "RecordSink",
    "SchemaDirectory",
    "SchemaSource",
    "Sink",
    "Stream",
    "StreamSchema",
    "Tap",
    "Target",
    "streams",
]


def __getattr__(name: str) -> t.Any:  # noqa: ANN401
    """Provide backward compatibility for SQL classes with deprecation warnings.

    Args:
        name: The name of the attribute to import.

    Returns:
        The imported attribute.

    Raises:
        AttributeError: If the attribute is not found.
    """
    if name == "SQLConnector":
        warnings.warn(
            f"Importing {name} from singer_sdk is deprecated. "
            f"Please import from singer_sdk.sql instead: "
            f"from singer_sdk.sql import {name}",
            SingerSDKDeprecationWarning,
            stacklevel=2,
        )
        from singer_sdk.sql import SQLConnector  # noqa: PLC0415

        return SQLConnector
    if name == "SQLSink":
        warnings.warn(
            f"Importing {name} from singer_sdk is deprecated. "
            f"Please import from singer_sdk.sql instead: "
            f"from singer_sdk.sql import {name}",
            SingerSDKDeprecationWarning,
            stacklevel=2,
        )
        from singer_sdk.sql import SQLSink  # noqa: PLC0415

        return SQLSink
    if name == "SQLStream":
        warnings.warn(
            f"Importing {name} from singer_sdk is deprecated. "
            f"Please import from singer_sdk.sql instead: "
            f"from singer_sdk.sql import {name}",
            SingerSDKDeprecationWarning,
            stacklevel=2,
        )
        from singer_sdk.sql import SQLStream  # noqa: PLC0415

        return SQLStream
    if name == "SQLTap":
        warnings.warn(
            f"Importing {name} from singer_sdk is deprecated. "
            f"Please import from singer_sdk.sql instead: "
            f"from singer_sdk.sql import {name}",
            SingerSDKDeprecationWarning,
            stacklevel=2,
        )
        from singer_sdk.sql import SQLTap  # noqa: PLC0415

        return SQLTap
    if name == "SQLTarget":
        warnings.warn(
            f"Importing {name} from singer_sdk is deprecated. "
            f"Please import from singer_sdk.sql instead: "
            f"from singer_sdk.sql import {name}",
            SingerSDKDeprecationWarning,
            stacklevel=2,
        )
        from singer_sdk.sql import SQLTarget  # noqa: PLC0415

        return SQLTarget

    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)
