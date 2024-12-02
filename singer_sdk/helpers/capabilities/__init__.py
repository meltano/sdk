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

#: Support the `ACTIVATE_VERSION <https://hub.meltano.com/singer/docs#activate-version>`_
#: extension.
#:
#: Example:
#:
#: .. code-block:: json
#:
#:    {
#:        "activate_version": true
#:    }
#:
ACTIVATE_VERSION = Builtin(schema=schema.ACTIVATE_VERSION_CONFIG)

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
#:
#: Example:
#:
#: .. code-block:: json
#:
#:    {
#:        "batch_config": {
#:            "encoding": {
#:                "format": "jsonl",
#:                "compression": "gzip"
#:            },
#:            "storage": {
#:                "type": "root",
#:                "root": "file:///path/to/batch/files",
#:                "prefix": "batch-"
#:            }
#:        }
#:    }
#:
BATCH = Builtin(
    schema=schema.BATCH_CONFIG,
    capability=PluginCapabilities.BATCH,
)

#: Support schema flattening, aka de-nesting of complex properties.
#:
#: Example:
#:
#: .. code-block:: json
#:
#:    {
#:        "flattening_enabled": true,
#:        "flattening_max_depth": 3
#:    }
#:
FLATTENING = Builtin(
    schema=schema.FLATTENING_CONFIG,
    capability=PluginCapabilities.FLATTENING,
)

#: Support inline stream map transforms.
#:
#: Example:
#:
#: .. code-block:: json
#:
#:    {
#:      "stream_maps": {
#:        "users": {
#:          "id": "id",
#:          "fields": "[f for f in fields if f['key'] != 'age']"
#:        }
#:      }
#:    }
#:
STREAM_MAPS = Builtin(
    schema.STREAM_MAPS_CONFIG,
    capability=PluginCapabilities.STREAM_MAPS,
)

#: Target batch size in rows.
#:
#: Example:
#:
#: .. code-block:: json
#:
#:    {
#:        "batch_size_rows": 10000
#:    }
#:
TARGET_BATCH_SIZE_ROWS = Builtin(schema=schema.TARGET_BATCH_SIZE_ROWS_CONFIG)

#: Support hard delete capability.
#:
#: Example:
#:
#: .. code-block:: json
#:
#:    {
#:        "hard_delete": true
#:    }
#:
TARGET_HARD_DELETE = Builtin(
    schema=schema.TARGET_HARD_DELETE_CONFIG,
    capability=TargetCapabilities.HARD_DELETE,
)

#: Target load method.
#:
#: Example:
#:
#: .. code-block:: json
#:
#:    {
#:        "load_method": "upsert"
#:    }
#:
TARGET_LOAD_METHOD = Builtin(schema=schema.TARGET_LOAD_METHOD_CONFIG)

#: Allow setting the target schema.
#:
#: Example:
#:
#: .. code-block:: json
#:
#:    {
#:        "default_target_schema": "my_schema"
#:    }
#:
TARGET_SCHEMA = Builtin(
    schema=schema.TARGET_SCHEMA_CONFIG,
    capability=TargetCapabilities.TARGET_SCHEMA,
)

#: Validate incoming records against their declared schema.
#:
#: Example:
#:
#: .. code-block:: json
#:
#:    {
#:        "validate_records": true
#:    }
#:
TARGET_VALIDATE_RECORDS = Builtin(
    schema=schema.TARGET_VALIDATE_RECORDS_CONFIG,
    capability=TargetCapabilities.VALIDATE_RECORDS,
)
