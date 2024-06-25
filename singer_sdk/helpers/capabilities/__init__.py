"""Module with helpers to declare capabilities and plugin behavior."""

from __future__ import annotations

from singer_sdk.helpers.capabilities import _schema as schema
from singer_sdk.helpers.capabilities._builtin import Builtin
from singer_sdk.helpers.capabilities._config_property import ConfigProperty
from singer_sdk.helpers.capabilities._enum import (
    CapabilitiesEnum,
    PluginCapabilities,
    TapCapabilities,
    TargetCapabilities,
    TargetLoadMethods,
)

__all__ = [
    "ADD_RECORD_METADATA",
    "BATCH",
    "FLATTENING",
    "STREAM_MAPS",
    "TARGET_BATCH_SIZE_ROWS",
    "TARGET_HARD_DELETE",
    "TARGET_LOAD_METHOD",
    "TARGET_SCHEMA",
    "TARGET_VALIDATE_RECORDS",
    "CapabilitiesEnum",
    "ConfigProperty",
    "PluginCapabilities",
    "TapCapabilities",
    "TargetCapabilities",
    "TargetLoadMethods",
]

#: Add metadata to records.
#:
#: Example:
#:
#: .. code-block:: json
#:
#:    {
#:        "add_record_metadata": true
#:    }
#:
ADD_RECORD_METADATA = Builtin(schema=schema.ADD_RECORD_METADATA_CONFIG)

#: For taps, support emitting BATCH messages. For targets, support consuming BATCH
#: messages.
BATCH = Builtin(
    schema=schema.BATCH_CONFIG,
    capability=PluginCapabilities.BATCH,
)

FLATTENING = Builtin(
    schema=schema.FLATTENING_CONFIG,
    capability=PluginCapabilities.FLATTENING,
)
STREAM_MAPS = Builtin(
    schema.STREAM_MAPS_CONFIG,
    capability=PluginCapabilities.STREAM_MAPS,
)
TARGET_BATCH_SIZE_ROWS = Builtin(schema=schema.TARGET_BATCH_SIZE_ROWS_CONFIG)
TARGET_HARD_DELETE = Builtin(
    schema=schema.TARGET_HARD_DELETE_CONFIG,
    capability=TargetCapabilities.HARD_DELETE,
)
TARGET_LOAD_METHOD = Builtin(schema=schema.TARGET_LOAD_METHOD_CONFIG)
TARGET_SCHEMA = Builtin(
    schema=schema.TARGET_SCHEMA_CONFIG,
    capability=TargetCapabilities.TARGET_SCHEMA,
)
TARGET_VALIDATE_RECORDS = Builtin(
    schema=schema.TARGET_VALIDATE_RECORDS_CONFIG,
    capability=TargetCapabilities.VALIDATE_RECORDS,
)
