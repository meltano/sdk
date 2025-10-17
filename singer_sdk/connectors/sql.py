"""Deprecated SQL connector module.

.. deprecated:: Next Release
    Import from singer_sdk.sql instead.
"""

from __future__ import annotations

import warnings

from singer_sdk.helpers._compat import SingerSDKDeprecationWarning

warnings.warn(
    "Importing from singer_sdk.connectors.sql is deprecated. "
    "Please import from singer_sdk.sql instead.",
    SingerSDKDeprecationWarning,
    stacklevel=2,
)

__all__ = [
    "FullyQualifiedName",
    "JSONSchemaToSQL",
    "JSONtoSQLHandler",
    "SQLConnector",
    "SQLToJSONSchema",
]

# Re-export for backward compatibility
from singer_sdk.sql.connector import (  # noqa: E402
    FullyQualifiedName,
    JSONSchemaToSQL,
    JSONtoSQLHandler,
    SQLConnector,
    SQLToJSONSchema,
)
