"""Deprecated SQL connector module.

.. deprecated:: Next Release
    Import from singer_sdk.sql instead.
"""

from __future__ import annotations

import typing as t
import warnings

from singer_sdk.helpers._compat import SingerSDKDeprecationWarning

if t.TYPE_CHECKING:
    from singer_sdk.sql import SQLConnector  # noqa: F401
    from singer_sdk.sql.connector import (
        FullyQualifiedName,  # noqa: F401
        JSONSchemaToSQL,  # noqa: F401
        JSONtoSQLHandler,  # noqa: F401
        SQLToJSONSchema,  # noqa: F401
    )


def __getattr__(name: str) -> t.Any:  # noqa: ANN401
    """Provide backward compatibility for moved SQL classes.

    Args:
        name: The name of the attribute to import.

    Returns:
        The imported attribute.

    Raises:
        AttributeError: If the attribute is not found.
    """
    msg_template = (
        "Importing {name} from singer_sdk.connectors.sql is deprecated and will be "  # noqa: RUF027
        "removed in the next release. Please import from singer_sdk.sql instead."
    )
    if name == "FullyQualifiedName":
        msg = msg_template.format(name=name)
        warnings.warn(msg, SingerSDKDeprecationWarning, stacklevel=2)
        from singer_sdk.sql.connector import FullyQualifiedName  # noqa: PLC0415

        return FullyQualifiedName

    if name == "JSONSchemaToSQL":
        msg = msg_template.format(name=name)
        warnings.warn(msg, SingerSDKDeprecationWarning, stacklevel=2)
        from singer_sdk.sql.connector import JSONSchemaToSQL  # noqa: PLC0415

        return JSONSchemaToSQL

    if name == "JSONtoSQLHandler":  # pragma: no cover
        msg = msg_template.format(name=name)
        warnings.warn(msg, SingerSDKDeprecationWarning, stacklevel=2)
        from singer_sdk.sql.connector import JSONtoSQLHandler  # noqa: PLC0415

        return JSONtoSQLHandler

    if name == "SQLConnector":
        msg = msg_template.format(name=name)
        warnings.warn(msg, SingerSDKDeprecationWarning, stacklevel=2)
        from singer_sdk.sql.connector import SQLConnector  # noqa: PLC0415

        return SQLConnector

    if name == "SQLToJSONSchema":
        msg = msg_template.format(name=name)
        warnings.warn(msg, SingerSDKDeprecationWarning, stacklevel=2)
        from singer_sdk.sql.connector import SQLToJSONSchema  # noqa: PLC0415

        return SQLToJSONSchema

    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)
